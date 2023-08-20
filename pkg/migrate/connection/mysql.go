package connection

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/baderkha/db-transfer/pkg/migrate/config/sourcecfg"
	"github.com/rs/zerolog"
	sqldblogger "github.com/simukti/sqldb-logger"
	"github.com/simukti/sqldb-logger/logadapter/zerologadapter"
)

func DialMysql(cfg *sourcecfg.MYSQL, maxConc int) *sql.DB {
	sqlDB, err := sql.Open("mysql", cfg.GetDSN())
	if err != nil {
		panic(fmt.Errorf("MYSQL_SOURCE : Could not dial connection to mysql due to : %w", err))
	}

	loggerAdapter := zerologadapter.New(zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.DateTime}))
	sqlDB = sqldblogger.OpenDriver(cfg.GetDSN(), sqlDB.Driver(), loggerAdapter,
		sqldblogger.WithWrapResult(false),
		sqldblogger.WithDurationFieldname("dur_ms"),
		sqldblogger.WithDurationUnit(sqldblogger.DurationMillisecond),
		sqldblogger.WithSQLQueryAsMessage(true),        // default: false
		sqldblogger.WithSQLQueryFieldname("sql_query"), // default: query
	) // default: LevelInfo)
	sqlDB.SetMaxOpenConns(maxConc)
	sqlDB.SetMaxIdleConns(maxConc)
	return sqlDB
}
