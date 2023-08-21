package targetcfg

import "fmt"

type S3Options struct {
	Bucket         string
	PrefixOverride string
}
type Snowflake struct {
	DB              string    `json:"db"`
	UserName        string    `json:"user_name"`
	Password        string    `json:"password"`
	Account         string    `json:"account"`
	IntegrationRole string    `json:"integeration_role"`
	Stage           string    `json:"stage"`
	Warehouse       string    `json:"ware_house"`
	Role            string    `json:"role"`
	S3              S3Options `json:"s3"`
	QueryLogging    bool      `json:"query_log"`
	Schema          string    `json:"schema"`
}

func (s *Snowflake) GetDSN() string {
	return fmt.Sprintf("%s:%s@%s.snowflakecomputing.com/%s/%s?protocol=https&role=%s&timezone=UTC&warehouse=%s", s.UserName, s.Password, s.Account, s.DB, s.Schema, s.Role, s.Warehouse)
}
