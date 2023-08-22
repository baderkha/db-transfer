package state

import (
	"fmt"
	"strconv"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type GormManager struct {
	DB *gorm.DB
}

func NewSqliteGormManager() *GormManager {
	db, err := gorm.Open(sqlite.Open("mydb.sqlite"), &gorm.Config{})
	if err != nil {
		fmt.Println("Error:", err)
		panic(err)
	}
	err = db.AutoMigrate(&RunLog{})
	if err != nil {
		panic(fmt.Errorf("Could not migrate %w", err))
	}
	err = db.AutoMigrate(&TableRunLog{})
	if err != nil {
		panic(fmt.Errorf("Could not migrate %w", err))
	}
	return &GormManager{DB: db}
}

func (m *GormManager) GetLastRun() *TableRunLog {
	var lastRun TableRunLog
	m.DB.Order("created_at desc").First(&lastRun)
	return &lastRun
}

func (m *GormManager) GetRunLog(runID string) *RunLog {
	var runLog RunLog
	m.DB.Where("run_id = ?", runID).First(&runLog)

	return &runLog
}

func (m *GormManager) GetTableRunLogs(runID string) []*TableRunLog {
	var tableRunLogs []*TableRunLog
	m.DB.Where("parent_run_id = ?", runID).Find(&tableRunLogs)
	return tableRunLogs
}

func (m *GormManager) InitRunLog(totalTableCount int) string {

	runLog := RunLog{
		TotalTablesForThisRun: totalTableCount,
		Status:                Started,
		Base:                  Base{CreatedAt: currentTime(), UpdatedAt: currentTime()},
	}
	m.DB.Create(&runLog)
	return strconv.Itoa(int(runLog.RunID))
}

func (m *GormManager) FailedRunLog(runID string, err error) {
	m.updateRunStatus(runID, Failed)
}

func (m *GormManager) PassedRunLog(runID string) {
	m.updateRunStatus(runID, Success)
}

func (m *GormManager) InitTableRunLog(runID string, dbName string, tableName string) {
	rID, err := strconv.Atoi(runID)
	if err != nil {
		panic(fmt.Errorf("Could not InitTableRunLog Expected runID to be an integer %w", err))
	}
	tableRunLog := TableRunLog{
		ParentRunID: rID,
		DBName:      dbName,
		TableName:   tableName,
		Status:      Started,
		Base:        Base{CreatedAt: currentTime(), UpdatedAt: currentTime()},
	}
	m.DB.Create(&tableRunLog)
}

func (m *GormManager) FailedTableRun(runID string, dbName string, tableName string, err error) {
	m.updateTableRunStatus(runID, dbName, tableName, Failed)
}

func (m *GormManager) PassedTableRun(runID string, dbName string, tableName string, rowsWritten int) {
	m.updateTableRunStatus(runID, dbName, tableName, Success)
}

func (m *GormManager) DidTableFailForRun(runID string) bool {
	var failedTableRunLogs int64
	m.DB.Model(&TableRunLog{}).Where("parent_run_id = ? AND status = ?", runID, Failed).Count(&failedTableRunLogs)
	return failedTableRunLogs > 0
}

func (m *GormManager) updateRunStatus(runID string, status RunLogState) {
	m.DB.Model(&RunLog{}).Where("run_id = ?", runID).Update("status", status)
	if status == Failed {
		m.DB.Model(&TableRunLog{}).Where("parent_run_id = ?", runID).Update("status", status)
	}
}

func (m *GormManager) updateTableRunStatus(runID string, dbName string, tableName string, status RunLogState) {
	m.DB.Model(&TableRunLog{}).Where("parent_run_id = ? AND db_name = ? AND table_name = ?", runID, dbName, tableName).Update("status", status)
}

func currentTime() *time.Time {
	now := time.Now()
	return &now
}
