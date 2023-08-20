package targetcfg

type S3Options struct {
	S3Bucket string `json:"s3_bucket"`
	S3Prefix string `json:"s3_prefix"`
	MaxRetry int    `json:"max_upload_retry"`
}

type Snowflake struct {
	DB              string `json:"db"`
	UserName        string `json:"user_name"`
	Password        string `json:"password"`
	Account         string `json:"account"`
	IntegrationRole string `json:"integeration_role"`
	Warehouse       string `json:"ware_house"`
	S3Options
}
