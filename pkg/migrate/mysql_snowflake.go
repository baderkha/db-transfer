package migrate

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/baderkha/db-transfer/pkg/conditional"
	"github.com/baderkha/db-transfer/pkg/migrate/config"
	"github.com/baderkha/db-transfer/pkg/migrate/config/sourcecfg"
	"github.com/baderkha/db-transfer/pkg/migrate/config/targetcfg"
	"github.com/baderkha/db-transfer/pkg/migrate/connection"
	"github.com/baderkha/db-transfer/pkg/migrate/state"
	"github.com/baderkha/db-transfer/pkg/migrate/table/colmap"
	"github.com/hashicorp/go-multierror"
	"github.com/kataras/tablewriter"
	"github.com/spf13/afero"

	"github.com/lensesio/tableprinter"
	"golang.org/x/sync/errgroup"
)

func NewMysqlToSnowflake() *MysqlToSnowflake {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"), // Specify your desired AWS region
	})

	if err != nil {
		panic(err)

	}
	b, err := os.ReadFile("resources/safe_col_mysql_snowflake.sql")
	if err != nil {
		panic(err)
	}
	printer := tableprinter.New(os.Stdout)
	printer.BorderTop, printer.BorderBottom, printer.BorderLeft, printer.BorderRight = true, true, true, true
	printer.CenterSeparator = "│"
	printer.ColumnSeparator = "│"
	printer.RowSeparator = "─"
	printer.HeaderBgColor = tablewriter.BgBlackColor
	printer.HeaderFgColor = tablewriter.FgGreenColor

	// Create a new S3 client
	return &MysqlToSnowflake{
		snowflakeWriteListener: make(chan snowflakeTargetEv),
		errWriteList:           make(chan error),
		fs:                     afero.NewOsFs(),
		targetFs:               s3.New(sess),
		safeCastQuery:          string(b),
		tbPrinter:              printer,
		stateMgr:               state.NewSqliteGormManager(),
	}
}

func TmpTablePfx(tableName string) string {
	return fmt.Sprintf("TEMP_MIGRATION_%s", tableName)
}

func UnPrefixTableName(prefixedTName string) string {
	return strings.ReplaceAll(prefixedTName, "TEMP_MIGRATION_", "")
}

type snowflakeTargetEv struct {
	LocalPathWrittenTo     string
	S3OPrefixPathWrittenTo string
	TableToWriteTo         string
	SchemaToWriteTo        string
	Die                    bool
	RowsWritten            int
}

type MysqlToSnowflake struct {
	source                 *sql.DB
	target                 *sql.DB
	targetFs               s3iface.S3API
	cfg                    config.Config[sourcecfg.MYSQL, targetcfg.Snowflake]
	snowflakeWriteListener chan snowflakeTargetEv
	errWriteList           chan error
	tmpDirPrefix           string
	s3DirPrefix            string
	runId                  string
	safeCastQuery          string
	dbColMap               map[string][]SafeColTypes
	fs                     afero.Fs
	tbPrinter              *tableprinter.Printer
	stateMgr               state.Manager
}

func (m *MysqlToSnowflake) GetStateManager() state.Manager {
	return m.stateMgr
}

func (m *MysqlToSnowflake) Init(cfg config.Config[sourcecfg.MYSQL, targetcfg.Snowflake]) error {
	m.cfg = cfg

	m.source = connection.DialMysql(cfg.SourceConfig.GetDSN(), cfg.MaxConcurrency, cfg.SourceConfig.QueryLogging)
	m.target = connection.DialSnowflake(cfg.Target.GetDSN(), cfg.Target.QueryLogging)

	m.s3DirPrefix = filepath.Join(conditional.Ternary(m.cfg.Target.S3.PrefixOverride != "", m.cfg.Target.S3.PrefixOverride, "./mysql_snowflake_migration"), "date="+time.Now().Format(time.DateOnly), "run_id="+m.runId)
	dbColMap, err := m.GetSafeColTypes()
	if err != nil {
		return fmt.Errorf("Could not read tap schema config due to %w", err)
	}
	if !cfg.SourceConfig.AllTables {
		dbColMap = m.getFilteredTableList(dbColMap)
	}

	if len(dbColMap) == 0 {
		return errors.New("No Tables to sync make sure your table config is valid")
	}
	m.dbColMap = dbColMap
	m.runId = m.stateMgr.InitRunLog(len(dbColMap))
	m.tmpDirPrefix = filepath.Join(conditional.Ternary(os.Getenv("WRITE_DIR") != "", os.Getenv("WRITE_DIR"), "./tmp"), "date="+time.Now().Format(time.DateOnly), "run_id="+m.runId)

	err = m.fs.MkdirAll(m.tmpDirPrefix, 0755)
	if err != nil {
		return err
	}

	// create table states
	err = m.InitTableRunState(dbColMap)
	if err != nil {
		return err
	}

	return nil

}

func (m *MysqlToSnowflake) getFilteredTableList(dbColMap map[string][]SafeColTypes) map[string][]SafeColTypes {
	var filteredListOfTables map[string][]SafeColTypes = make(map[string][]SafeColTypes)
	for _, v := range m.cfg.SourceConfig.TableList {
		cols, ok := dbColMap[v.JoinName()]
		if ok {
			filteredListOfTables[v.JoinName()] = cols
		} else {
			fmt.Printf("Warning : %s does not exist in the schema omitting ....\n", v.JoinName())
		}
	}
	dbColMap = filteredListOfTables
	return dbColMap
}

// table_schema as db_name,
//
//						table_name as tb_name,
//						column_name AS column_name,
//	                  data_type AS data_type,
//	                  column_type AS column_type,
//	                  safe_sql_value AS safe_sql_value
type SafeColTypes struct {
	DBName     string
	TableName  string
	ColumnName string
	DataType   string
	ColumnType string
	SafeSQL    string
	TargetType string
}

func (m *MysqlToSnowflake) GetSafeColTypes() (map[string][]SafeColTypes, error) {
	var (
		res map[string][]SafeColTypes = make(map[string][]SafeColTypes)
	)

	rows, err := m.source.Query(m.safeCastQuery)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var sct SafeColTypes
		err = rows.Scan(&sct.DBName, &sct.TableName, &sct.ColumnName, &sct.DataType, &sct.ColumnType, &sct.SafeSQL)
		if err != nil {
			return nil, err
		}
		res[sct.DBName+"."+sct.TableName] = append(res[sct.DBName+"."+sct.TableName], sct)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return res, nil
}

func (m *MysqlToSnowflake) Run(cfg config.Config[sourcecfg.MYSQL, targetcfg.Snowflake]) error {
	err := m.Init(cfg)
	if err != nil {
		return err
	}
	var (
		sflakeWg sync.WaitGroup
		sourceWg errgroup.Group
		dbColMap = m.dbColMap
	)

	defer m.CleanUp()

	sourceWg.SetLimit(m.cfg.MaxConcurrency)
	// listener will be running in the bg waiting for files to be written
	m.ListenToSnowflakeFiles(&sflakeWg)

	dbColMap, err = m.GenerateTargetCast(dbColMap)
	if err != nil {
		return err
	}

	m.dbColMap = dbColMap

	err = m.GenerateTmpTables(dbColMap)
	if err != nil {
		return err
	}

	for k, v := range dbColMap {
		sourceWg.Go(func(k string, v []SafeColTypes) func() error {
			return func() error {
				dbName, tbName := m.parseTBNameFromMapKey(k)
				err := m.HandleTableDump(k, v)
				if err != nil {
					m.stateMgr.FailedTableRun(m.runId, dbName, tbName, fmt.Errorf("Failed Streaming Table to File : %w", err))
					return err
				}
				return nil
			}
		}(k, v))
	}

	err = sourceWg.Wait()
	if err != nil {
		m.stateMgr.FailedRunLog(m.runId, fmt.Errorf("Failed Table Streaming due to : %w", err))
		return err
	}
	fmt.Println("finished streaming tables")
	m.ShutSflakeDownWorkers()
	sflakeWg.Wait() // wait once the other is done
	if !m.stateMgr.DidTableFailForRun(m.runId) {
		m.stateMgr.PassedRunLog(m.runId)
	}
	return nil
}

func (m *MysqlToSnowflake) InitTableRunState(dbColMap map[string][]SafeColTypes) error {
	for k := range dbColMap {
		dbName, tbName := m.parseTBNameFromMapKey(k)
		m.stateMgr.InitTableRunLog(m.runId, dbName, tbName)

	}

	if len(m.stateMgr.GetTableRunLogs(m.runId)) != len(dbColMap) {
		return fmt.Errorf("State Error, tables in state != len of tables to sync")
	}
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

func (m *MysqlToSnowflake) HandleTableDump(DBTableName string, safeCols []SafeColTypes) error {

	fmt.Printf("STREAMING TABLE %s \n", DBTableName)
	var (
		DBName, TBName  = m.parseTBNameFromMapKey(DBTableName)
		result          []interface{}
		batchAt         = 0
		ctr             = 0
		currentFile     afero.File
		subPrefix       = filepath.Join("db="+DBName, "tb_name="+TBName)
		prefix          = filepath.Join(m.tmpDirPrefix, subPrefix)
		cols            []string
		currentFileName string = fmt.Sprintf("%s_%d.csv", TBName, batchAt)
		err             error
		wg              errgroup.Group
		rowsWritten     int
	)
	wg.SetLimit(m.cfg.MaxConcurrency)
	for _, col := range safeCols {
		cols = append(cols, col.SafeSQL)
	}

	_ = m.fs.MkdirAll(prefix, 0755)
	currentFile, err = m.fs.Create(filepath.Join(prefix, currentFileName))
	if err != nil {
		return err
	}
	csvWriter := csv.NewWriter(currentFile)

	rows, err := m.source.Query(fmt.Sprintf(`SELECT %s FROM %s WHERE 1=1`, strings.Join(cols, ","), WrapQ(DBName)+"."+WrapQ(TBName)))
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
		rowsWritten += 1

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
			currentFile.Seek(0, 0)
			oldFile := currentFile
			currentFile = nil
			wg.Go(func(s3Prefix string, subPrefix string, currentFileName string, cfg targetcfg.Snowflake) func() error {
				return func() error {
					defer oldFile.Close()
					key := filepath.Join(s3Prefix, subPrefix, currentFileName)
					_, err := m.targetFs.PutObject(&s3.PutObjectInput{
						Bucket: &cfg.S3.Bucket,
						Body:   oldFile,
						Key:    &key,
					})

					return err
				}
			}(m.s3DirPrefix, subPrefix, currentFileName, m.cfg.Target))

			if !isLastRow {
				ctr = 0
				batchAt += m.cfg.BatchRecordSize
				currentFileName = fmt.Sprintf("%s_%d.csv", TBName, batchAt)
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
	err = wg.Wait()
	if err != nil {
		return err
	}
	fmt.Println("here before publish " + TBName + " " + DBName)
	// done processing this table
	m.PublishSnowflakeWriteEv(snowflakeTargetEv{
		TableToWriteTo:         TBName,
		SchemaToWriteTo:        DBName,
		S3OPrefixPathWrittenTo: filepath.Join(m.s3DirPrefix, subPrefix),
		LocalPathWrittenTo:     prefix,
		RowsWritten:            rowsWritten,
	})
	fmt.Println("here after publish " + TBName + " " + DBName)
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
		err := em.handleSnowflakeEv(event)
		if err != nil {
			em.stateMgr.FailedTableRun(em.runId, event.SchemaToWriteTo, event.TableToWriteTo, fmt.Errorf("Failed Writing to Snowflake due to %w", err))
		} else {
			em.stateMgr.PassedTableRun(em.runId, event.SchemaToWriteTo, event.TableToWriteTo, event.RowsWritten)
		}
		fmt.Printf("Worker %d finished processing event: %v\n", workerID, event)
	}
}

func (em *MysqlToSnowflake) handleSnowflakeEv(event snowflakeTargetEv) error {
	defer em.fs.RemoveAll(event.LocalPathWrittenTo)
	_, err := em.target.Exec(fmt.Sprintf(`
			COPY INTO "%s.%s"
			FROM %s/%s/
		`,
		event.SchemaToWriteTo, TmpTablePfx(event.TableToWriteTo),
		em.cfg.Target.Stage,
		event.S3OPrefixPathWrittenTo,
	))
	if err != nil {
		return err
	}
	em.target.Exec(fmt.Sprintf(`ALTER TABLE IF EXISTS "%s.%s" RENAME TO "%s.%s_bak_old"`, event.SchemaToWriteTo, event.TableToWriteTo, event.SchemaToWriteTo, event.TableToWriteTo))
	_, err = em.target.Exec(fmt.Sprintf(`ALTER TABLE "%s.%s" RENAME TO "%s.%s"`, event.SchemaToWriteTo, TmpTablePfx(event.TableToWriteTo), event.SchemaToWriteTo, event.TableToWriteTo))
	if err != nil {
		return fmt.Errorf(`Could not Rename Table from "%s.%s" RENAME TO "%s.%s" due to : %w`, event.SchemaToWriteTo, TmpTablePfx(event.TableToWriteTo), event.SchemaToWriteTo, event.TableToWriteTo, err)
	}
	_, _ = em.target.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s.%s_bak_old"`, event.SchemaToWriteTo, event.TableToWriteTo))
	return nil
}

func (m *MysqlToSnowflake) parseTBNameFromMapKey(mapKey string) (db string, table string) {
	ar := strings.Split(mapKey, ".")
	return ar[0], ar[1]
}

func (m *MysqlToSnowflake) GenerateTmpTables(inf map[string][]SafeColTypes) error {
	var (
		wg errgroup.Group
	)
	wg.SetLimit(4) // no more than 4 go routines
	for k, v := range inf {
		wg.Go(func(k string, v []SafeColTypes) func() error {
			return func() error {
				var cols []string
				for _, c := range v {
					cols = append(cols, fmt.Sprintf(`"%s" %s`, c.ColumnName, c.TargetType))
				}
				sql := fmt.Sprintf(`
			CREATE OR REPLACE TABLE "%s" (
				%s
			)`,
					k,
					strings.Join(cols, ",\n"),
				)
				_, err := m.target.Exec(sql)
				if err != nil {
					return fmt.Errorf("COULD NOT CREATE TEMPORARY TABLE %s due to %w", k, err)
				}
				return nil
			}
		}(k, v),
		)

	}
	return wg.Wait()
}

func (m *MysqlToSnowflake) GenerateTargetCast(inf map[string][]SafeColTypes) (map[string][]SafeColTypes, error) {
	var finalErr error
	for k, v := range inf {
		for i := range v {
			convertedField, err := colmap.Convert(colmap.MysqlToSnowflake, v[i].DataType)
			if err != nil {
				finalErr = multierror.Append(finalErr, fmt.Errorf("Cast Error : Bad Casting for %s for column %s due to : %w", k, v[i].ColumnName, err))
				continue
			}
			v[i].TargetType = convertedField
		}
		inf[k] = v
	}
	if finalErr != nil {
		return nil, finalErr
	}
	return inf, nil
}

func (m *MysqlToSnowflake) CleanUp() {
	defer close(m.snowflakeWriteListener)
	defer m.source.Close()
	defer m.target.Close()
}
