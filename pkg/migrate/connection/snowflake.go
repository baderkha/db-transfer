package connection

import (
	"database/sql"
	"fmt"

	_ "github.com/snowflakedb/gosnowflake"
)

func DialSnowflake(dsn string, qlog bool) *sql.DB {
	fmt.Println("getting DialSnowflake")
	var res string
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		panic(err)
	}

	if qlog {
		db = AddLogger(db, dsn, "snowflake")
	}
	row := db.QueryRow("SELECT 1")

	if row.Err() != nil {
		panic(err)
	}
	row.Scan(&res)
	if res != "1" {
		panic("can't ping snowflake via select 1")
	}
	fmt.Println("got DialSnowflake")
	return db
}
