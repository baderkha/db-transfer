package table

type TableInformation struct {
	Name           string
	Type           string
	IsPrimaryIndex bool
}

type ColumnTypes struct {
	ColumnName string `db:"col_name"`
	Type       string `db:"col_type"`
	TargetType string `db:"target_type"`
}

type Index struct {
	ColumnName   string `db:"col_name"`
	IndexName    string `db:"index_name"`
	IsPrimaryKey bool   `db:"is_primary_key"`
}

type Info struct {
	TableName    string `db:"table_name"`
	DatabaseName string `db:"db_name"`
	Schema       []*ColumnTypes
	Indexes      []*Index
	SizeMB       float64
}

type InfoSortBy string
type InfoSortByDirection string

const (
	SortBySize           InfoSortBy = "Size"
	SortByAlphaTableName InfoSortBy = "TableName"
)

const (
	SortDirectionASC  InfoSortByDirection = "ASC"
	SortDirectionDESC InfoSortByDirection = "DESC"
)

type FetchOptions struct {
	SortByCol       InfoSortBy
	SortByDirection InfoSortByDirection
}

type InfoFetcher interface {
	// fetches the schema and also does conversion on the type to the target db
	All(f *FetchOptions) ([]*Info, error)
}
