package sourcecfg

import (
	"fmt"
)

type MYSQL struct {
	SessionVariableValues map[string]string `json:"session_vars"`
	TableList             []string          `json:"table_list"`
	Host                  string            `json:"host"`
	UserName              string            `json:"user_name"`
	Password              string            `json:"password"`
	Port                  int               `json:"port"`
	DB                    string            `json:"db"`
	QueryLogging          bool              `json:"query_log"`
}

func (m *MYSQL) GetDSN() string {
	var ses []string
	for k, v := range m.SessionVariableValues {
		ses = append(ses, k+"="+v)
	}
	return fmt.Sprintf(`%s:%s@tcp(%s:%d)/%s?parseTime=true&collation=utf8mb4_general_ci&autocommit=true`, m.UserName, m.Password, m.Host, m.Port, m.DB)
}
