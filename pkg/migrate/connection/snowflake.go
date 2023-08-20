package connection

import (
	"database/sql"

	_ "github.com/snowflakedb/gosnowflake"
)

func DialSnowflake(dsn string) *sql.DB {
	var res string
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		panic(err)
	}

	db = AddLogger(db, dsn)
	row := db.QueryRow("SELECT 1")

	if row.Err() != nil {
		panic(err)
	}
	row.Scan(&res)
	if res != "1" {
		panic("select 1 did not work for snowflake")
	}
	return db
}
