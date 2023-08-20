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
	Warehouse       string    `json:"ware_house"`
	Role            string    `json:"role"`
	S3              S3Options `json:"s3"`
}

func (s *Snowflake) GetDSN() string {
	return fmt.Sprintf("%s:%s@%s.snowflakecomputing.com/%s?protocol=https&role=%s&timezone=UTC&warehouse=%s", s.UserName, s.Password, s.Account, s.DB, s.Role, s.Warehouse)
}
