package targetcfg

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
	S3              S3Options `json:"s3"`
}
