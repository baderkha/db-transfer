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
		"TINYINT":            "NUMBER",
		"SMALLINT":           "NUMBER",
		"MEDIUMINT":          "NUMBER",
		"INT":                "NUMBER",
		"BIGINT":             "NUMBER",
		"FLOAT":              "FLOAT",
		"DOUBLE":             "FLOAT",
		"DECIMAL":            "NUMBER",
		"DATE":               "DATE",
		"TIME":               "TIME",
		"DATETIME":           "TIMESTAMP",
		"TIMESTAMP":          "TIMESTAMP",
		"YEAR":               "NUMBER",
		"CHAR":               "STRING",
		"VARCHAR":            "STRING",
		"BINARY":             "BINARY",
		"VARBINARY":          "BINARY",
		"BLOB":               "BINARY",
		"TEXT":               "STRING",
		"LONGTEXT":           "STRING",
		"MEDIUMTEXT":         "STRING",
		"ENUM":               "STRING",
		"SET":                "STRING",
		"JSON":               "VARIANT",
		"JSONP":              "VARIANT",
		"GEOMETRY":           "VARIANT",
		"POINT":              "VARIANT",
		"LINESTRING":         "VARIANT",
		"POLYGON":            "VARIANT",
		"GEOMETRYCOLLECTION": "VARIANT",
		"MULTIPOLYGON":       "VARIANT",
		"MULTIPOINT":         "VARIANT",
		"MULTILINESTRING":    "VARIANT",
		"BIT":                "NUMBER",
		"BOOLEAN":            "BOOLEAN",
		"SERIAL":             "NUMBER",
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
