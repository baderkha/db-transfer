package main

import (
	"fmt"
	"time"

	"github.com/baderkha/db-transfer/pkg/migrate"
	"github.com/baderkha/db-transfer/pkg/migrate/config"
	"github.com/baderkha/db-transfer/pkg/migrate/config/sourcecfg"
	"github.com/baderkha/db-transfer/pkg/migrate/config/targetcfg"
	"github.com/davecgh/go-spew/spew"
)

func main() {
	// "db_user_name": "root",
	// "db_password": "password",
	// "db_schema": "main",
	// "db_port": "6000",
	// "db_host": "127.0.0.1",
	// "db_params": "",
	// "db_query_log": "TRUE",
	startTime := time.Now()
	spew.Dump(migrate.
		NewMysqlToSnowflake().
		Run(config.Config[sourcecfg.MYSQL, targetcfg.Snowflake]{
			MaxConcurrency:  8,
			BatchRecordSize: 10000,
			SourceConfig: sourcecfg.MYSQL{
				Host:     "127.0.0.1",
				UserName: "root",
				Password: "password",
				Port:     6000,
				DB:       "main",
			},
			Target: targetcfg.Snowflake{},
		}))
	endTime := time.Now()

	// Calculate the time taken
	elapsedTime := endTime.Sub(startTime)

	fmt.Printf("Time taken: %s\n", elapsedTime)
}
