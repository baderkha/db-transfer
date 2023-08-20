package targetcfg

type Snowflake struct {
	DB              string `json:"db"`
	UserName        string `json:"user_name"`
	Password        string `json:"password"`
	Account         string `json:"account"`
	IntegrationRole string `json:"integeration_role"`
	Warehouse       string `json:"ware_house"`
}
