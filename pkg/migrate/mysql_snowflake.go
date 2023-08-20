package migrate

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/baderkha/db-transfer/pkg/conditional"
	"github.com/baderkha/db-transfer/pkg/migrate/config"
	"github.com/baderkha/db-transfer/pkg/migrate/config/sourcecfg"
	"github.com/baderkha/db-transfer/pkg/migrate/config/targetcfg"
	"github.com/baderkha/db-transfer/pkg/migrate/connection"
	"github.com/baderkha/db-transfer/pkg/migrate/table"
	"github.com/baderkha/db-transfer/pkg/migrate/table/colmap"
	"github.com/gofrs/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/spf13/afero"
)

func NewMysqlToSnowflake() *MysqlToSnowflake {
	uid, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}
	return &MysqlToSnowflake{
		runId:         uid.String(),
		csvWriteChan:  make(chan csvWrittenEv),
		errWriteList:  make(chan error),
		statusChan:    make(chan statusEv),
		TableDumpChan: make(chan tableDumpEv),
		fs:            afero.NewOsFs(),
		tmpDirPrefix:  filepath.Join(conditional.Ternary(os.Getenv("WRITE_DIR") != "", os.Getenv("WRITE_DIR"), "./tmp"), "date=", "run_id="+uid.String()),
		s3DirPrefix:   filepath.Join(conditional.Ternary(os.Getenv("S3_PREFIX") != "", os.Getenv("S3_PREFIX"), "./files"), "date=", "run_id="+uid.String()),
		targetFs:      s3.New(session.Must(session.NewSession(aws.NewConfig()))),
	}
}

func PrefixTableName(tableName string) string {
	return fmt.Sprintf("TEMP_MIGRATION_%s", tableName)
}

func UnPrefixTableName(prefixedTName string) string {
	return strings.ReplaceAll(prefixedTName, "TEMP_MIGRATION_", "")
}

type csvWrittenEv struct {
	FilePathWritten string
	TableToWriteTo  string
	SchemaToWriteTo string
	IsDone          bool
	Die             bool
}

type tableDumpEv struct {
	table.Info
	Die bool
}

type statusEv struct {
	HasCompletedTable bool
	Err               error
}

type MysqlToSnowflake struct {
	source          *sql.DB
	target          *sql.DB
	targetFs        s3iface.S3API
	infoFetcher     table.InfoFetcher
	cfg             config.Config[sourcecfg.MYSQL, targetcfg.Snowflake]
	TableDumpChan   chan tableDumpEv
	csvWriteChan    chan csvWrittenEv
	errWriteList    chan error
	statusChan      chan statusEv
	tablesCompleted int
	tablesToDump    int
	tmpDirPrefix    string
	s3DirPrefix     string
	runId           string
	finalErr        error
	fs              afero.Fs
}

func (m *MysqlToSnowflake) Init(cfg config.Config[sourcecfg.MYSQL, targetcfg.Snowflake]) {
	m.cfg = cfg
	m.source = connection.DialMysql(&cfg.SourceConfig, cfg.MaxConcurrency)
	m.target = nil
	m.infoFetcher = table.NewInfoFetcherMysql(m.source)
}

func (m *MysqlToSnowflake) Run(cfg config.Config[sourcecfg.MYSQL, targetcfg.Snowflake]) error {
	var (
		sflakeWg   sync.WaitGroup
		sourceWg   sync.WaitGroup
		fatalErrWg sync.WaitGroup
	)
	m.Init(cfg)
	defer m.CleanUp()
	err := m.fs.MkdirAll(m.tmpDirPrefix, 0755)
	if err != nil {
		return err
	}
	// listener will be running in the bg waiting for files to be written
	m.ListToTableDumpEv(&sourceWg)
	m.ListenToCSVWriteEv(&sflakeWg)
	m.ListenToFatalErr(&fatalErrWg)

	allTableInfo, err :=
		m.
			infoFetcher.
			All(&table.FetchOptions{
				SortByCol:       table.SortBySize,
				SortByDirection: table.SortDirectionDESC,
			})
	allTableInfo, err = m.GenerateTargetCast(allTableInfo)
	if err != nil {
		return err
	}
	if len(allTableInfo) == 0 {
		return nil
	}
	m.tablesToDump = len(allTableInfo)

	for _, v := range allTableInfo {
		m.PublishTableDumpEv(tableDumpEv{
			Info: *v,
			Die:  false,
		})
	}

	err = m.GenerateTargetTables(allTableInfo)
	if err != nil {
		return err
	}

	sourceWg.Wait()
	sflakeWg.Wait()
	fatalErrWg.Wait()
	return m.finalErr
}

func WrapQ(sql string) string {
	return "`" + sql + "`"
}

func (m *MysqlToSnowflake) ShutDownCSVListener() {
	for i := 0; i < m.cfg.MaxConcurrency; i++ {
		m.PublishCSVWriteEv(csvWrittenEv{
			Die: true,
		})
	}
}

func (m *MysqlToSnowflake) ShutDownTableDumpListener() {
	for i := 0; i < m.cfg.MaxConcurrency; i++ {
		m.PublishTableDumpEv(tableDumpEv{
			Die: true,
		})
	}
}

func (m *MysqlToSnowflake) HandleTableDump(a *table.Info) error {
	var (
		result          []interface{}
		batchAt         = 0
		ctr             = 0
		currentFile     afero.File
		prefix          = filepath.Join(m.tmpDirPrefix, a.DatabaseName, a.TableName)
		cols            []string
		currentFileName string = fmt.Sprintf("%s_%d.csv", a.TableName, batchAt)
		err             error
	)
	for _, col := range a.Schema {
		cols = append(cols, WrapQ(col.ColumnName))
	}

	_ = m.fs.MkdirAll(prefix, 0755)
	currentFile, err = m.fs.Create(filepath.Join(prefix, currentFileName))
	if err != nil {
		return err
	}
	csvWriter := csv.NewWriter(currentFile)

	rows, err := m.source.Query(fmt.Sprintf(`SELECT %s FROM %s WHERE 1=1`, strings.Join(cols, ","), WrapQ(a.DatabaseName)+"."+WrapQ(a.TableName)))
	if err != nil {
		return err
	}

	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("Error getting columns:", err)
	}
	for range columns {
		var val sql.RawBytes
		result = append(result, &val)
	}
	isLastRow := !rows.Next()
	for !isLastRow {
		ctr += 1

		if err := rows.Scan(result...); err != nil {
			log.Fatal("Error scanning row:", err)
		}

		rowData := make([]string, len(result))
		for i, val := range result {
			rowData[i] = string(*val.(*sql.RawBytes))
		}
		err = csvWriter.Write(rowData)
		if err != nil {
			return err
		}

		isLastRow = !rows.Next()
		if ctr >= m.cfg.BatchRecordSize || isLastRow {
			csvWriter.Flush()
			currentFile.Close()
			finishedFilePath := filepath.Join(prefix, currentFileName)
			m.PublishCSVWriteEv(csvWrittenEv{
				FilePathWritten: finishedFilePath,
				TableToWriteTo:  a.TableName,
				SchemaToWriteTo: a.DatabaseName,
				IsDone:          false,
			})
			if !isLastRow {
				ctr = 0
				batchAt += m.cfg.BatchRecordSize
				currentFileName = fmt.Sprintf("%s_%d.csv", a.TableName, batchAt)
				currentFile, err = m.fs.Create(filepath.Join(prefix, currentFileName))
				if err != nil {
					return err
				}

				csvWriter = csv.NewWriter(currentFile)
			}
		}

	}
	if err := rows.Err(); err != nil {
		return err
	}
	// done processing this table
	m.PublishCSVWriteEv(csvWrittenEv{
		TableToWriteTo:  a.TableName,
		SchemaToWriteTo: a.DatabaseName,
		IsDone:          true,
	})
	return nil
}

func (m *MysqlToSnowflake) PublishTableDumpEv(event tableDumpEv) {
	m.TableDumpChan <- event
}

func (m *MysqlToSnowflake) PublishCSVWriteEv(event csvWrittenEv) {
	m.csvWriteChan <- event
}

func (m *MysqlToSnowflake) PublishStatusChangeEv(event statusEv) {
	m.statusChan <- event
}

func (m *MysqlToSnowflake) ListenToFatalErr(wg *sync.WaitGroup) {
	wg.Add(1)
	go m.OnStatusChange(wg)
}

func (m *MysqlToSnowflake) ListenToCSVWriteEv(wg *sync.WaitGroup) {
	for i := 1; i <= m.cfg.MaxConcurrency; i++ {
		wg.Add(1)
		go m.OnCsvWritten(i, wg)
	}
}

func (m *MysqlToSnowflake) ListToTableDumpEv(wg *sync.WaitGroup) {
	for i := 1; i <= m.cfg.MaxConcurrency; i++ {
		wg.Add(1)
		go m.OnTableDump(i, wg)
	}
}

func (em *MysqlToSnowflake) OnStatusChange(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		event, ok := <-em.statusChan
		if !ok || event.Err != nil || em.tablesCompleted == em.tablesToDump {
			em.finalErr = event.Err
			em.ShutDownTableDumpListener()
			em.ShutDownCSVListener()
			return
		} else if event.HasCompletedTable {
			em.tablesCompleted += 1
		}
	}
}

func (em *MysqlToSnowflake) OnTableDump(workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		event, ok := <-em.TableDumpChan // Wait for an event to be published
		if !ok || event.Die {
			fmt.Printf("Worker %d exiting\n", workerID)
			return
		}
		err := em.HandleTableDump(&event.Info)
		if err != nil {
			em.PublishStatusChangeEv(statusEv{
				HasCompletedTable: false,
				Err:               err,
			})

		}

	}
}

func (em *MysqlToSnowflake) OnCsvWritten(workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		event, ok := <-em.csvWriteChan // Wait for an event to be published
		if !ok || event.Die {
			fmt.Printf("Worker %d exiting\n", workerID)
			return
		}
		if !event.IsDone {
			var (
				f afero.File
			)
			f, err := em.fs.Open(event.FilePathWritten)

			if err != nil {
				em.PublishStatusChangeEv(statusEv{
					Err: err,
				})
				continue
			}
			defer f.Close()
			err = em.UploadFile(f, em.cfg.Target.S3Bucket, filepath.Join(em.s3DirPrefix, filepath.Base(event.FilePathWritten)))
			if err != nil {
				em.PublishStatusChangeEv(statusEv{
					Err: fmt.Errorf("%s.%s : %w", event.SchemaToWriteTo, event.TableToWriteTo, err)},
				)
				continue
			}

		} else {
			em.PublishStatusChangeEv(statusEv{
				HasCompletedTable: true,
			})
		}

	}
}

func (em *MysqlToSnowflake) UploadFile(f afero.File, Bucket string, Key string) error {
	var (
		retryCtr int
		err      error
	)
	for retryCtr < em.cfg.Target.MaxRetry {
		_, err = em.targetFs.PutObject(&s3.PutObjectInput{
			Body:   f,
			Bucket: &em.cfg.Target.S3Bucket,
			Key:    &Key,
		})
		if err == nil {
			return nil
		}
		retryCtr++
	}
	return fmt.Errorf("Attemted uploading key (%s) %d times with no success this is a failure and will be treated as a fatal event : original_err=%w", Key, retryCtr+1, err)
}

func (m *MysqlToSnowflake) GenerateTargetTables(inf []*table.Info) error {
	return nil
}

func (m *MysqlToSnowflake) GenerateTargetCast(inf []*table.Info) ([]*table.Info, error) {
	var finalErr error
	for _, v := range inf {
		for i := range v.Schema {
			convertedField, err := colmap.Convert(colmap.MysqlToSnowflake, v.Schema[i].Type)
			if err != nil {
				finalErr = multierror.Append(finalErr, fmt.Errorf("Cast Error : Bad Casting for %s.%s for column %s due to : %w", v.DatabaseName, v.TableName, v.Schema[i].ColumnName, err))
				continue
			}
			v.Schema[i].TargetType = convertedField
		}
	}
	if finalErr != nil {
		return nil, finalErr
	}
	return inf, nil
}

func (m *MysqlToSnowflake) CleanUp() {
	defer close(m.csvWriteChan)
	defer m.source.Close()
	//defer m.target.Close()
}
