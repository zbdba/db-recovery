package ibdata

import (
	"fmt"

	"github.com/zbdba/db-recovery/recovery/logs"
	"github.com/zbdba/db-recovery/recovery/utils"
)

// TODO: no used function
func (parse *Parse) getColumnType(ColumnName string, columns []Columns) uint64 {
	for _, f := range columns {
		if f.FieldName == ColumnName {
			return f.FieldType
		}
	}
	return 0
}

// TODO: read table struct info use sql parse,
//  user should identify the create table sql statement.
func (parse *Parse) GetTableFieldsFromStruct() {
	// TODO: need sql parser
}

// TODO: recovery table structure from system data dictionary.
func (parse *Parse) RecoveryTableStruct(path string, DBName string, TableName string) (string, error) {
	_, ParseFileErr := parse.parseFile(path)
	if ParseFileErr != nil {
		return "", ParseFileErr
	}

	// Get table fields.
	table, GetFieldsErr := parse.getTableFromDict(DBName, TableName)
	if GetFieldsErr != nil {
		return "", GetFieldsErr
	}

	CreateTableSql := parse.MakeCreateTableSql(table)

	return CreateTableSql, nil
}

// TODO: use column info to make create table sql statement.
func (parse *Parse) MakeCreateTableSql(table Tables) string {
	return ""
}

// TODO: parse undo info from system data file.
func (parse *Parse) ParseUndoPageHeader(page *Page) *Page {

	d := page.data
	var pos uint64
	pos = 0

	TRX_UNDO_PAGE_TYPE := utils.MatchReadFrom2(d[pos:])
	fmt.Println("the TRX_UNDO_PAGE_TYPE is ", TRX_UNDO_PAGE_TYPE)
	pos += 2

	TRX_UNDO_PAGE_START := utils.MatchReadFrom2(d[pos:])
	fmt.Println("the TRX_UNDO_PAGE_START is ", TRX_UNDO_PAGE_START)
	pos += 2

	TRX_UNDO_PAGE_FREE := utils.MatchReadFrom2(d[pos:])
	fmt.Println("the TRX_UNDO_PAGE_FREE is ", TRX_UNDO_PAGE_FREE)
	pos += 2

	pos += 12

	// The undo log segment header
	TRX_UNDO_STATE := utils.MatchReadFrom2(d[pos:])
	fmt.Println("the TRX_UNDO_STATE is ", TRX_UNDO_STATE)
	pos += 2

	TRX_UNDO_LAST_LOG := utils.MatchReadFrom2(d[pos:])
	fmt.Println("the TRX_UNDO_LAST_LOG is ", TRX_UNDO_LAST_LOG)
	pos += 2

	//TRX_UNDO_FSEG_HEADER := utils.MatchReadFrom2(d[pos:])
	//fmt.Println("the TRX_UNDO_FSEG_HEADER is ", TRX_UNDO_FSEG_HEADER)
	//pos +=2

	// TRX_UNDO_PAGE_LIST
	pos += 10

	pos += 16

	// Start parse undo log (undo log header and data)
	TRX_UNDO_TRX_ID := utils.MatchReadFrom8(d[pos:])
	fmt.Println("the TRX_UNDO_TRX_ID is ", TRX_UNDO_TRX_ID)
	pos += 8

	TRX_UNDO_TRX_NO := utils.MatchReadFrom8(d[pos:])
	fmt.Println("the TRX_UNDO_TRX_NO is ", TRX_UNDO_TRX_NO)
	pos += 8

	TRX_UNDO_DEL_MARKS := utils.MatchReadFrom2(d[pos:])
	fmt.Println("the TRX_UNDO_DEL_MARKS is ", TRX_UNDO_DEL_MARKS)
	pos += 2

	TRX_UNDO_LOG_START := utils.MatchReadFrom2(d[pos:])
	fmt.Println("the TRX_UNDO_LOG_START is ", TRX_UNDO_LOG_START)
	pos += 2

	TRX_UNDO_XID_EXISTS := utils.MatchReadFrom1(d[pos:])
	fmt.Println("the TRX_UNDO_XID_EXISTS is ", TRX_UNDO_XID_EXISTS)
	pos++

	TRX_UNDO_DICT_TRANS := utils.MatchReadFrom1(d[pos:])
	fmt.Println("the TRX_UNDO_DICT_TRANS is ", TRX_UNDO_DICT_TRANS)
	pos++

	TRX_UNDO_TABLE_ID := utils.MatchReadFrom8(d[pos:])
	fmt.Println("the TRX_UNDO_TABLE_ID is ", TRX_UNDO_TABLE_ID)
	pos += 8

	TRX_UNDO_NEXT_LOG := utils.MatchReadFrom2(d[pos:])
	fmt.Println("the TRX_UNDO_NEXT_LOG is ", TRX_UNDO_NEXT_LOG)
	pos += 2

	TRX_UNDO_PREV_LOG := utils.MatchReadFrom2(d[pos:])
	fmt.Println("the TRX_UNDO_PREV_LOG is ", TRX_UNDO_PREV_LOG)
	pos += 2

	// the undo log start at TRX_UNDO_LOG_START.
	tmp_pos := TRX_UNDO_LOG_START - 38
	DataLen := utils.MatchReadFrom2(d[tmp_pos:])
	tmp_pos += 2
	StartPos := tmp_pos

	fmt.Println(DataLen, StartPos)

	TypeCmpl := int64(utils.MatchReadFrom1(d[tmp_pos:]))
	tmp_pos++

	TypeCmpl &= ^128
	UndoType := TypeCmpl & (16 - 1)
	CmplInfo := TypeCmpl

	// don't parse insert record
	if UndoType == 11 ||
		UndoType == 13 || UndoType == 14 {
		logs.Debug("UndoType is", UndoType, "CmplInfo is ", CmplInfo, " unused log type, pass.")
	}

	logs.Debug("UndoType is", UndoType, "CmplInfo is ", CmplInfo)

	UndoNo, err := utils.MatchUllReadMuchCompressed(d[tmp_pos:])
	if err != nil {
	}
	logs.Debug("UndoNo is ", UndoNo)

	//pos += 1
	num := utils.MachUllGetMuchCompressedSize(UndoNo)
	tmp_pos += num

	TableId, err := utils.MatchUllReadMuchCompressed(d[tmp_pos:])
	fmt.Println("table id is ", TableId)

	//tmp_pos1 := DataLen
	//DataLen1 := utils.MatchReadFrom2(d[DataLen:])
	//fmt.Println(DataLen1)
	//tmp_pos1 += 2
	//
	//TypeCmpl1 := int64(utils.MatchReadFrom1(d[tmp_pos1:]))
	//tmp_pos1 += 1
	//
	//TypeCmpl1 &= ^128
	//UndoType1 := TypeCmpl1 & (16 - 1)
	//CmplInfo1 := TypeCmpl1
	//
	//// don't parse insert record
	//if UndoType1 == 11 ||
	//	UndoType1 == 13 || UndoType1 == 14 {
	//	logs.Debug("UndoType is", UndoType1, "CmplInfo is ", CmplInfo1, " unused log type, pass.")
	//}
	//
	//logs.Debug("UndoType is", UndoType1, "CmplInfo is ", CmplInfo1)
	//
	//UndoNo1, err := utils.MatchUllReadMuchCompressed(d[tmp_pos1:])
	//if err != nil {
	//}
	//logs.Debug("UndoNo is ", UndoNo1)
	//
	////pos += 1
	//num1 := utils.MachUllGetMuchCompressedSize(UndoNo1)
	//tmp_pos1 += num1;
	//
	//TableId1, err := utils.MatchUllReadMuchCompressed(d[tmp_pos1:])
	//fmt.Println("table id is ", TableId1)

	return page
}
