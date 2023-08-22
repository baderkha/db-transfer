package state

import "time"

type RunLogState string

const (
	Started RunLogState = "STARTED"
	Success RunLogState = "SUCCESS"
	Aborted RunLogState = "ABORTED"
	Failed  RunLogState = "FAILED"
)

type Base struct {
	CreatedAt *time.Time `json:"created_at" db:"created_at"`
	UpdatedAt *time.Time `json:"updated_at" db:"updated_at"`
}

type RunLog struct {
	RunID                 int         `json:"run_id" db:"run_id" gorm:"primaryKey;autoIncrement"`
	TotalTablesForThisRun int         `json:"total_tables_for_run" db:"total_tables_for_run"`
	Status                RunLogState `json:"status" db:"status" gorm:"type:varchar(50)"`
	Base
}

type TableRunLog struct {
	ParentRunID int         `json:"parent_run_id" db:"parent_run_id"`
	DBName      string      `json:"db_name" db:"db_name" gorm:"type:varchar(255)"`
	TableName   string      `json:"table_name" db:"table_name" gorm:"type:varchar(255)"`
	RowWritten  int         `json:"rows_written_target" gorm:"type:int(11)"`
	Status      RunLogState `gorm:"type:varchar(50)"`
	Base
}

type Manager interface {
	// This should sort by most recent first
	GetLastRun() *TableRunLog
	// GetRunLog : GetRunLog get a specific run log
	GetRunLog(runID string) *RunLog
	GetTableRunLogs(runID string) []*TableRunLog
	// InitRunLog : start a run log
	InitRunLog(totalTableCount int) (runID string)
	FailedRunLog(runID string, err error)
	PassedRunLog(runID string)
	InitTableRunLog(runID string, dbName string, tableName string)
	FailedTableRun(runID string, dbName string, tableName string, err error)
	PassedTableRun(runID string, dbName string, tableName string, rowsWritten int)
	DidTableFailForRun(runID string) bool
}
