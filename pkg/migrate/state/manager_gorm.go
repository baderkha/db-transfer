package state

import (
	"fmt"
	"strconv"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var (
	db *gorm.DB
)

type GormManager struct {
	DB *gorm.DB
}

func NewSqliteGormManager() *GormManager {
	if db == nil {
		db2, err := gorm.Open(sqlite.Open("mydb.sqlite"), &gorm.Config{})
		if err != nil {
			fmt.Println("Error:", err)
			panic(err)
		}
		err = db2.AutoMigrate(&RunLog{})
		if err != nil {
			panic(fmt.Errorf("Could not migrate %w", err))
		}
		err = db2.AutoMigrate(&TableRunLog{})
		if err != nil {
			panic(fmt.Errorf("Could not migrate %w", err))
		}
		db = db2
	}
	return &GormManager{DB: db}
}

func (m *GormManager) OnShutDownEv() {
	run := m.GetLastRun()
	tx := m.DB.Begin()
	if run == nil {
		return
	} else if run.Status == Started {
		fmt.Printf("Last Run had status as %s , moving that to %s INSTEAD ... \n", Started, Aborted)
		errTx := tx.Model(&RunLog{}).Where("run_id = ? AND status =?", run.RunID, Started).Updates(RunLog{
			Status: Aborted,
		}).Error
		if errTx != nil {
			tx.Rollback()
		}
		errTx = tx.Model(&TableRunLog{}).Where("parent_run_id = ? and status = ?", run.RunID, Started).Update("status", Aborted).Error
		if errTx != nil {
			tx.Rollback()
		}
		tx.Commit()
	}

}

func (m *GormManager) GetLastRun() *RunLog {
	var lastRun RunLog
	m.DB.Order("created_at desc").First(&lastRun)
	if lastRun.RunID == 0 && lastRun.CreatedAt.IsZero() {
		return nil
	}
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
	m.updateRunStatus(runID, Failed, err)
}

func (m *GormManager) PassedRunLog(runID string) {
	m.updateRunStatus(runID, Success, nil)
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
	m.updateTableRunStatus(runID, dbName, tableName, Failed, 0, err)
}

func (m *GormManager) PassedTableRun(runID string, dbName string, tableName string, rowsWritten int) {
	m.updateTableRunStatus(runID, dbName, tableName, Success, rowsWritten, nil)
}

func (m *GormManager) DidTableFailForRun(runID string) bool {
	var failedTableRunLogs int64
	m.DB.Model(&TableRunLog{}).Where("parent_run_id = ? AND status = ?", runID, Failed).Count(&failedTableRunLogs)
	return failedTableRunLogs > 0
}

func (m *GormManager) updateRunStatus(runID string, status RunLogState, err error) {
	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}
	tx := m.DB.Begin()
	errTx := tx.Model(&RunLog{}).Where("run_id = ?", runID).Updates(RunLog{
		Status: status,
		ErrMsg: errMsg,
	}).Error
	if errTx != nil {
		tx.Rollback()
	}
	if status == Failed {
		errTx = tx.Model(&RunLog{}).Where("parent_run_id = ? and status = ?", runID, Started).Update("status", Aborted).Error
		if errTx != nil {
			tx.Rollback()
		}
	}
	tx.Commit()
}

func (m *GormManager) updateTableRunStatus(runID string, dbName string, tableName string, status RunLogState, rows int, err error) {
	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}
	m.DB.Model(&TableRunLog{}).Where("parent_run_id = ? AND db_name = ? AND table_name = ?", runID, dbName, tableName).Updates(&TableRunLog{
		Status:     status,
		ErrMsg:     errMsg,
		RowWritten: rows,
	})
}

func currentTime() *time.Time {
	now := time.Now()
	return &now
}
