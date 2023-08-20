// package colmap
//
// maps columns between different database types
package colmap

import (
	"fmt"
	"strings"
)

// Type : column mapping type
type Type string

const (
	// MysqlToSnowflake : mysql -> snowflake type casting
	MysqlToSnowflake Type = "MYSQL_SNOWFLAKE"
)

var (
	mysqlToSnowflakeMap = map[string]string{
		"CHAR":               "VARCHAR",
		"VARCHAR":            "VARCHAR",
		"BINARY":             "BINARY",
		"VARBINARY":          "BINARY",
		"BLOB":               "VARCHAR",
		"TINYBLOB":           "VARCHAR",
		"MEDIUMBLOB":         "VARCHAR",
		"LONGBLOB":           "VARCHAR",
		"GEOMETRY":           "VARIANT",
		"POINT":              "VARIANT",
		"LINESTRING":         "VARIANT",
		"POLYGON":            "VARIANT",
		"MULTIPOINT":         "VARIANT",
		"MULTILINESTRING":    "VARIANT",
		"MULTIPOLYGON":       "VARIANT",
		"GEOMETRYCOLLECTION": "VARIANT",
		"TEXT":               "VARCHAR",
		"TINYTEXT":           "VARCHAR",
		"MEDIUMTEXT":         "VARCHAR",
		"LONGTEXT":           "VARCHAR",
		"ENUM":               "VARCHAR",
		"INT":                "NUMBER",
		"TINYINT":            "BOOLEAN",
		"SMALLINT":           "NUMBER",
		"MEDIUMINT":          "NUMBER",
		"BIGINT":             "NUMBER",
		"BIT":                "BOOLEAN",
		"DECIMAL":            "FLOAT",
		"DOUBLE":             "FLOAT",
		"FLOAT":              "FLOAT",
		"BOOL":               "BOOLEAN",
		"BOOLEAN":            "BOOLEAN",
		"DATE":               "TIMESTAMP_NTZ",
		"DATETIME":           "TIMESTAMP_NTZ",
		"TIMESTAMP":          "TIMESTAMP_NTZ",
		"TIME":               "TIME",
		"JSON":               "VARIANT",
	}
)

// Convert : converts types to the target db if it cannot then it will error out
func Convert(t Type, colTypeSource string) (string, error) {
	colTypeSource = strings.ToUpper(strings.Split(colTypeSource, "(")[0])
	switch t {
	case MysqlToSnowflake:
		itm, ok := mysqlToSnowflakeMap[colTypeSource]
		if !ok {
			return "", fmt.Errorf("This col type %s does not have a snowflake mapping", colTypeSource)
		}
		return itm, nil
	}
	return "", fmt.Errorf("Unsupported type %s", t)
}

// MustConvert : if the conversion errors out it panics
func MustConvert(t Type, colTypeSource string) string {
	val, err := Convert(t, colTypeSource)
	if err != nil {
		panic(fmt.Errorf("%s : Could not cast %s", MysqlToSnowflake, colTypeSource))
	}
	return val
}
