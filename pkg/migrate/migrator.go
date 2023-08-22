package migrate

import "github.com/baderkha/db-transfer/pkg/migrate/config"

// Runner : runs migration between a tap and source
type Runner[T any, S any] interface {
	Run(cfg config.Config[T, S]) error // fresh run
	RetryFailedTablesLastRun() error   // attempts to recover if run failed last time
}
