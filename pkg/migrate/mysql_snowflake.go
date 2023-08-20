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
	"golang.org/x/sync/errgroup"
)

func NewMysqlToSnowflake() *MysqlToSnowflake {
	uid, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}
	return &MysqlToSnowflake{
		runId:                  uid.String(),
		snowflakeWriteListener: make(chan snowflakeTargetEv),
		errWriteList:           make(chan error),
		fs:                     afero.NewOsFs(),
		tmpDirPrefix:           filepath.Join(conditional.Ternary(os.Getenv("WRITE_DIR") != "", os.Getenv("WRITE_DIR"), "./tmp"), "date=", "run_id="+uid.String()),
	}
}

func PrefixTableName(tableName string) string {
	return fmt.Sprintf("TEMP_MIGRATION_%s", tableName)
}

func UnPrefixTableName(prefixedTName string) string {
	return strings.ReplaceAll(prefixedTName, "TEMP_MIGRATION_", "")
}

type snowflakeTargetEv struct {
	FilePathWritten string
	TableToWriteTo  string
	SchemaToWriteTo string
	IsDone          bool
	Die             bool
}

type MysqlToSnowflake struct {
	source                 *sql.DB
	target                 *sql.DB
	targetFs               s3iface.S3API
	infoFetcher            table.InfoFetcher
	cfg                    config.Config[sourcecfg.MYSQL, targetcfg.Snowflake]
	snowflakeWriteListener chan snowflakeTargetEv
	errWriteList           chan error
	tmpDirPrefix           string
	runId                  string
	fs                     afero.Fs
}

func (m *MysqlToSnowflake) Init(cfg config.Config[sourcecfg.MYSQL, targetcfg.Snowflake]) {
	m.cfg = cfg
	m.source = connection.DialMysql(&cfg.SourceConfig, cfg.MaxConcurrency)
	m.target = nil
	m.infoFetcher = table.NewInfoFetcherMysql(m.source)
}

func (m *MysqlToSnowflake) Run(cfg config.Config[sourcecfg.MYSQL, targetcfg.Snowflake]) error {
	var (
		sflakeWg sync.WaitGroup
		sourceWg errgroup.Group
	)
	m.Init(cfg)
	defer m.CleanUp()
	err := m.fs.MkdirAll(m.tmpDirPrefix, 0755)
	if err != nil {
		return err
	}
	sourceWg.SetLimit(m.cfg.MaxConcurrency)
	// listener will be running in the bg waiting for files to be written
	m.ListenToSnowflakeFiles(&sflakeWg)

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

	err = m.GenerateTargetTables(allTableInfo)
	if err != nil {
		return err
	}

	for _, v := range allTableInfo {
		sourceWg.Go(func(info *table.Info) func() error {
			return func() error {
				return m.HandleTableDump(info)
			}
		}(v))
	}

	err = sourceWg.Wait()
	if err != nil {
		return err
	}
	m.ShutSflakeDownWorkers()
	sflakeWg.Wait() // wait once the other is done
	return nil
}

func WrapQ(sql string) string {
	return "`" + sql + "`"
}

func (m *MysqlToSnowflake) ShutSflakeDownWorkers() {
	for i := 0; i < m.cfg.MaxConcurrency; i++ {
		m.PublishSnowflakeWriteEv(snowflakeTargetEv{
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

		if ctr >= m.cfg.BatchRecordSize || isLastRow {
			csvWriter.Flush()
			currentFile.Close()
			finishedFilePath := filepath.Join(prefix, currentFileName)
			m.PublishSnowflakeWriteEv(snowflakeTargetEv{
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
		isLastRow = !rows.Next()
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// done processing this table
	m.PublishSnowflakeWriteEv(snowflakeTargetEv{
		IsDone: true,
	})
	return nil
}

func (m *MysqlToSnowflake) PublishSnowflakeWriteEv(event snowflakeTargetEv) {
	m.snowflakeWriteListener <- event
}

func (m *MysqlToSnowflake) ListenToSnowflakeFiles(wg *sync.WaitGroup) {
	for i := 1; i <= m.cfg.MaxConcurrency; i++ {
		wg.Add(1)
		go m.OnSnowflakeCSVFileEv(i, wg)
	}
}

func (em *MysqlToSnowflake) OnSnowflakeCSVFileEv(workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		event, ok := <-em.snowflakeWriteListener // Wait for an event to be published
		if !ok || event.Die {
			fmt.Printf("Worker %d exiting\n", workerID)
			return
		}
		fmt.Printf("Worker %d received event: %v\n", workerID, event)
	}
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
	defer close(m.snowflakeWriteListener)
	defer m.source.Close()
	//defer m.target.Close()
}
