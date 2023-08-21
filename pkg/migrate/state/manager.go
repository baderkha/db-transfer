package state

import "time"

type RunLogState string

const (
	Started RunLogState = "STARTED"
	Success RunLogState = "SUCCESS"
	Failed  RunLogState = "FAILED"
)

type Base struct {
	CreatedAt *time.Time `json:"created_at" db:"created_at"`
	UpdatedAt *time.Time `json:"updated_at" db:"updated_at"`
}

type RunLog struct {
	RunID                 string      `json:"run_id" db:"run_id"`
	TotalTablesForThisRun int         `json:"total_tables_for_run" db:"total_tables_for_run"`
	Status                RunLogState `json:"status" db:"status"`
	Base
}

type TableRunLog struct {
	ParentRunID string `json:"parent_run_id" db:"parent_run_id"`
	DBName      string `json:"db_name" db:"db_name"`
	TableName   string `json:"table_name" db:"table_name"`
	RowWritten  int    `json:"rows_written_target"`
	Status      RunLogState
	Base
}

type Manager interface {
	// This should sort by most recent first
	GetLastRun() *TableRunLog
	// GetRunLog : GetRunLog get a specific run log
	GetRunLog(runID string) *RunLog
	GetTableRunLogs(runID string) []*TableRunLog
	// InitRunLog : start a run log
	InitRunLog(runID string, totalTableCount int)
	FailedRunLog(runID string)
	PassedRunLog(runID string)
	InitTableRunLog(runID string, dbName string, tableName string)
	FailedTableRun(runID string, dbName string, tableName string)
	PassedTableRun(runID string, dbName string, tableName string)
}
