package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/baderkha/db-transfer/pkg/migrate"
	"github.com/baderkha/db-transfer/pkg/migrate/config"
	"github.com/baderkha/db-transfer/pkg/migrate/config/sourcecfg"
	"github.com/baderkha/db-transfer/pkg/migrate/config/targetcfg"
	"github.com/baderkha/db-transfer/pkg/migrate/state"
)

var (
	runErr error
)

func waitForInterrupt(mger state.Manager) {
	// Create a channel to receive interrupt signals
	interruptChannel := make(chan os.Signal, 1)
	signal.Notify(interruptChannel, os.Interrupt, syscall.SIGTERM)

	// Run indefinitely and respond to the signal
	for {
		select {
		case <-interruptChannel:
			fmt.Println("Interrupt received. Stopping gracefully...")
			mger.OnShutDownEv()
			os.Exit(130)
		default:
			// Continue processing
		}
	}
}

func main() {

	startTime := time.Now()
	var cfg config.Config[sourcecfg.MYSQL, targetcfg.Snowflake]

	b, err := os.ReadFile("job.json")
	if err != nil {
		panic(err)
	}
	migrator := migrate.NewMysqlToSnowflake()
	go waitForInterrupt(migrator.GetStateManager())
	err = json.Unmarshal(b, &cfg)
	if err != nil {
		panic(err)
	}
	endTime := time.Now()

	runErr = migrator.Run(cfg)

	// Calculate the time taken
	elapsedTime := endTime.Sub(startTime)

	fmt.Printf("Time taken: %s\n", elapsedTime)
}
