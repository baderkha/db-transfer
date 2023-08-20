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
	// Create a new S3 client
	return &MysqlToSnowflake{
		runId:                  uid.String(),
		snowflakeWriteListener: make(chan snowflakeTargetEv),
		errWriteList:           make(chan error),
		fs:                     afero.NewOsFs(),
		targetFs:               s3.New(sess),
		safeCastQuery:          string(b),
	}
}

func PrefixTableName(tableName string) string {
	return fmt.Sprintf("TEMP_MIGRATION_%s", tableName)
}

func UnPrefixTableName(prefixedTName string) string {
	return strings.ReplaceAll(prefixedTName, "TEMP_MIGRATION_", "")
}

type snowflakeTargetEv struct {
	S3OPrefixPathWrittenTo string
	TableToWriteTo         string
	SchemaToWriteTo        string
	Die                    bool
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
	s3DirPrefix            string
	runId                  string
	safeCastQuery          string
	safeCols               map[string][]SafeColTypes
	fs                     afero.Fs
}

func (m *MysqlToSnowflake) Init(cfg config.Config[sourcecfg.MYSQL, targetcfg.Snowflake]) {
	m.cfg = cfg
	m.source = connection.DialMysql(cfg.SourceConfig.GetDSN(), cfg.MaxConcurrency)
	m.target = connection.DialSnowflake(cfg.Target.GetDSN())
	m.infoFetcher = table.NewInfoFetcherMysql(m.source)
	m.tmpDirPrefix = filepath.Join(conditional.Ternary(os.Getenv("WRITE_DIR") != "", os.Getenv("WRITE_DIR"), "./tmp"), "date="+time.Now().Format(time.DateOnly), "run_id="+m.runId)
	m.s3DirPrefix = filepath.Join(conditional.Ternary(m.cfg.Target.S3.PrefixOverride != "", m.cfg.Target.S3.PrefixOverride, "./mysql_snowflake_migration"), "date="+time.Now().Format(time.DateOnly), "run_id="+m.runId)

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

	safeCols, err := m.GetSafeColTypes()
	if err != nil {
		return err
	}
	m.safeCols = safeCols

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
	fmt.Println("finished streaming tables")
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
		subPrefix       = filepath.Join("db="+a.DatabaseName, "tb_name="+a.TableName)
		prefix          = filepath.Join(m.tmpDirPrefix, subPrefix)
		cols            []string
		currentFileName string = fmt.Sprintf("%s_%d.csv", a.TableName, batchAt)
		err             error
		wg              errgroup.Group
	)
	wg.SetLimit(m.cfg.MaxConcurrency)
	safeCols := m.safeCols[a.DatabaseName+"."+a.TableName]
	for _, col := range safeCols {
		cols = append(cols, col.SafeSQL)
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
	err = wg.Wait()
	if err != nil {
		return err
	}
	fmt.Println("here before publish " + a.TableName + " " + a.DatabaseName)
	// done processing this table
	m.PublishSnowflakeWriteEv(snowflakeTargetEv{
		TableToWriteTo:         a.TableName,
		SchemaToWriteTo:        a.DatabaseName,
		S3OPrefixPathWrittenTo: filepath.Join(m.s3DirPrefix, subPrefix),
	})
	fmt.Println("here after publish " + a.TableName + " " + a.DatabaseName)
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
		time.Sleep(1 * time.Second)
		fmt.Printf("Worker %d finished processing event: %v\n", workerID, event)

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
