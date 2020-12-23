package ibdata

import (
	"bytes"
	"fmt"

	"github.com/zbdba/db-recovery/recovery/logs"
	"github.com/zbdba/db-recovery/recovery/utils"
)

// Make row data to replace into statement, it will be more convenient when restoring data.
func (parse *Parse) makeReplaceIntoStatement(AllColumns [][]Columns, table string, database string) {
	var buf bytes.Buffer
	var query string

	fmt.Println("Print the format sql statement: ")

	for _, columns := range AllColumns {

		buf.WriteString(fmt.Sprintf("replace into `%s`.`%s` values (", database, table))
		firstCol := true

		for _, column := range columns {

			// Skip internal field.
			if column.FieldName == "DB_ROW_ID" ||
				column.FieldName == "DB_TRX_ID" ||
				column.FieldName == "DB_ROLL_PTR" {
				continue
			}

			if firstCol {
				firstCol = false
			} else {
				buf.WriteByte(',')
			}

			if column.MySQLType == utils.MYSQL_TYPE_BIT {
				buf.WriteString("b")
			}

			if column.IsBinary {
				buf.WriteString("unhex(")
			}

			if column.FieldValue != nil && column.FieldValue != "NULL" {
				buf.WriteByte('\'')
				buf.WriteString(utils.EscapeValue(fmt.Sprintf("%v", column.FieldValue)))
				buf.WriteByte('\'')
			} else {
				//buf.WriteByte('\'')
				//buf.WriteByte('\'')
				buf.WriteString("NULL")
			}

			if column.IsBinary {
				buf.WriteString(")")
			}
		}

		buf.WriteString(");")
		query = buf.String()
		buf.Reset()

		fmt.Println(query)
		logs.Debug("query is ", query)

	}

}
