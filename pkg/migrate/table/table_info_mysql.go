package table

import (
	"database/sql"
	"sort"

	"golang.org/x/sync/errgroup"
)

func NewInfoFetcherMysql(db *sql.DB) InfoFetcher {
	return &InfoFetcherMYSQL{
		source: db,
	}
}

type InfoFetcherMYSQL struct {
	source *sql.DB
}

type mysqlTableSizes struct {
	Table  string  `db:"tb_name"`
	SizeMB float64 `db:"size_mb"`
}

func (m *InfoFetcherMYSQL) All(f *FetchOptions) ([]*Info, error) {
	var (
		res      []*Info
		tSizesMp map[string]*mysqlTableSizes = make(map[string]*mysqlTableSizes)
		wg       errgroup.Group
		isAsc    bool       = false
		sortCol  InfoSortBy = f.SortByCol
	)

	if f.SortByDirection == SortDirectionASC {
		isAsc = true
	}

	wg.Go(func() error {
		rows, err := m.source.Query(`
	select table_schema as db_name ,
    table_name
	from information_schema.tables
	where table_type = 'BASE TABLE'
		and table_schema not in ('information_schema','mysql','performance_schema','sys')
		order by db_name, table_name
	`)
		if err != nil {
			return err
		}

		for rows.Next() {
			var ifo Info
			rows.Scan(&ifo.DatabaseName, &ifo.TableName)
			res = append(res, &ifo)
		}
		return nil
	})
	wg.Go(func() error {
		tSizes := m.GetAllTableSizes()
		for _, v := range tSizes {
			tSizesMp[v.Table] = v
		}
		return nil
	})

	err := wg.Wait()
	if err != nil {
		return nil, err
	}

	for i, _ := range res {
		var (
			schma []*ColumnTypes
			idxs  []*Index
			wg    errgroup.Group
		)

		res[i].SizeMB = tSizesMp[res[i].TableName].SizeMB

		wg.Go(
			func() error {
				idxs = m.GetTableIndicies(res[i].DatabaseName, res[i].TableName)
				return nil
			})
		wg.Go(func() error {
			schma = m.GetTableInfo(res[i].DatabaseName, res[i].TableName)
			return nil
		})
		wg.Wait()

		res[i].Schema = schma
		res[i].Indexes = idxs
	}
	if sortCol != "" {
		sort.Slice(res, func(i, j int) bool {
			if sortCol == SortBySize {
				return res[i].SizeMB < res[j].SizeMB && isAsc
			} else {
				return res[i].TableName < res[j].TableName && isAsc
			}
		})
	}
	return res, nil
}

func (m *InfoFetcherMYSQL) GetAllTableSizes() []*mysqlTableSizes {
	var res []*mysqlTableSizes
	rows, err := m.source.Query(`SELECT
	TABLE_NAME AS tb_name,
	ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024),4) AS size_mb
  FROM
	information_schema.TABLES
  WHERE
	TABLE_SCHEMA not in ('information_schema','mysql',
								  'performance_schema','sys')
  ORDER BY
	(DATA_LENGTH + INDEX_LENGTH)
  DESC`)
	if err != nil {
		return nil
	}
	for rows.Next() {
		var ifo mysqlTableSizes
		rows.Scan(&ifo.Table, &ifo.SizeMB)
		res = append(res, &ifo)
	}

	return res
}

func (m *InfoFetcherMYSQL) GetTableInfo(dbName string, table string) []*ColumnTypes {
	var res []*ColumnTypes
	rows, err := m.source.Query(`SELECT COLUMN_NAME AS col_name, COLUMN_TYPE AS col_type
	FROM information_schema.COLUMNS  
	WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`, dbName, table)
	if err != nil {
		return nil
	}
	for rows.Next() {
		var ifo ColumnTypes
		rows.Scan(&ifo.ColumnName, &ifo.Type)
		res = append(res, &ifo)
	}
	return res
}

func (m *InfoFetcherMYSQL) GetTableIndicies(dbName string, table string) []*Index {
	var res []*Index
	rows, err := m.source.Query(`
	SELECT
		COLUMN_NAME as col_name,
		INDEX_NAME as index_name,
		(case WHEN lower(INDEX_NAME) = 'primary' then true ELSE false END) as is_primary
	FROM
		INFORMATION_SCHEMA.STATISTICS
	WHERE
		TABLE_SCHEMA = ?
		AND TABLE_NAME = ?
`, dbName, table)
	if err != nil {
		return nil
	}
	for rows.Next() {
		var ifo Index
		_ = rows.Scan(&ifo.ColumnName, &ifo.IndexName, &ifo.IsPrimaryKey)
		res = append(res, &ifo)
	}

	return res
}
