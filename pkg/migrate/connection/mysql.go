package connection

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/rs/zerolog"
	sqldblogger "github.com/simukti/sqldb-logger"
	"github.com/simukti/sqldb-logger/logadapter/zerologadapter"
)

func AddLogger(db *sql.DB, dsn string, driverName string) *sql.DB {
	loggerAdapter := zerologadapter.New(zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.DateTime}).With().Str("driver", driverName).Logger())
	db = sqldblogger.OpenDriver(dsn, db.Driver(), loggerAdapter,
		sqldblogger.WithWrapResult(false),
		sqldblogger.WithDurationFieldname("dur_ms"),
		sqldblogger.WithDurationUnit(sqldblogger.DurationMillisecond),
		sqldblogger.WithSQLQueryAsMessage(true),        // default: false
		sqldblogger.WithSQLQueryFieldname("sql_query"), // default: query
	) // default: LevelInfo)
	return db
}

func DialMysql(dsn string, maxConc int, qlog bool) *sql.DB {
	fmt.Println("getting DialMysql con")
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(fmt.Errorf("MYSQL_SOURCE : Could not dial connection to mysql due to : %w", err))
	}
	if qlog {
		sqlDB = AddLogger(sqlDB, dsn, "mysql")
	}
	sqlDB.SetMaxOpenConns(maxConc)
	sqlDB.SetMaxIdleConns(maxConc)
	fmt.Println("got DialMysql con")
	return sqlDB
}
