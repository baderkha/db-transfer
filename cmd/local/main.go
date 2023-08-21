package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/baderkha/db-transfer/pkg/migrate"
	"github.com/baderkha/db-transfer/pkg/migrate/config"
	"github.com/baderkha/db-transfer/pkg/migrate/config/sourcecfg"
	"github.com/baderkha/db-transfer/pkg/migrate/config/targetcfg"
)

func main() {

	startTime := time.Now()
	var cfg config.Config[sourcecfg.MYSQL, targetcfg.Snowflake]

	b, err := os.ReadFile("job.json")
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(b, &cfg)
	if err != nil {
		panic(err)
	}
	endTime := time.Now()
	migrate.
		NewMysqlToSnowflake().
		Run(cfg)

	// Calculate the time taken
	elapsedTime := endTime.Sub(startTime)

	fmt.Printf("Time taken: %s\n", elapsedTime)
}
