// Copyright 2019 The zbdba Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redo

import (
	"fmt"
	"strings"

	"github.com/zbdba/db-recovery/recovery/ibdata"
	"github.com/zbdba/db-recovery/recovery/logs"
	"github.com/zbdba/db-recovery/recovery/utils"
)

func (parse *Parse) MLOG_N_BYTES(data []byte, pos *uint64, MyType uint64) error {

	offset := utils.MatchReadFrom2(data[*pos:])
	logs.Debug("offset is", offset, "pos is ", *pos, " data len is ", len(data))

	*pos += 2

	if MyType == MLOG_8BYTES {
		value, _, err := utils.MatchParseCompressed(data, *pos)
		if err != nil {
			return err
		}

		size, err := utils.MatchGetCompressedSize(value)
		if err != nil {
			return err
		}
		*pos += size
		*pos += 4
	} else {
		_, num, err := utils.MatchParseCompressed(data, *pos)
		*pos += num
		if err != nil {
			return err
		}
	}
	return nil
}

func (parse *Parse) MLOG_REC_SEC_DELETE_MARK(data []byte, pos *uint64) {

	value := utils.MatchReadFrom1(data[*pos:])
	*pos++

	offset := utils.MatchReadFrom2(data[*pos:])
	*pos += 2

	logs.Debug("values is ", value, " offset is ", offset)
}

// Parse the undo record.
func (parse *Parse) MLOG_UNDO_INSERT(data []byte, pos *uint64) error {

	DataLen := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	StartPos := *pos

	// catch panic
	defer func() {
		if err := recover(); err != nil {
			logs.Error("skip error, the error is ", err)
			*pos = StartPos + DataLen
		}
	}()

	TypeCmpl := int64(utils.MatchReadFrom1(data[*pos:]))
	*pos++

	TypeCmpl &= ^TRX_UNDO_UPD_EXTERN
	UndoType := TypeCmpl & (TRX_UNDO_CMPL_INFO_MULT - 1)
	CmplInfo := TypeCmpl

	logs.Debug("UndoType is", UndoType, "CmplInfo is ", CmplInfo)

	UndoNo, err := utils.MatchUllReadMuchCompressed(data[*pos:])
	if err != nil {
		*pos = StartPos + DataLen
		return err
	}
	logs.Debug("UndoNo is ", UndoNo)

	*pos += utils.MachUllGetMuchCompressedSize(UndoNo)

	TableId, err := utils.MatchUllReadMuchCompressed(data[*pos:])
	if err != nil {
		*pos = StartPos + DataLen
		return err
	}

	logs.Debug("TableId is ", TableId)
	*pos += utils.MachUllGetMuchCompressedSize(TableId)

	if UndoType != TRX_UNDO_INSERT_REC {
		// Skip info bits
		logs.Debug("info bits is ", data[*pos:][0])
		*pos++

		TrxId := utils.MatchUllReadComPressed(data, pos)
		logs.Debug("TrxID is ", TrxId)

		RollPtr := utils.MatchUllReadComPressed(data, pos)
		logs.Debug("RollPtr is ", RollPtr)
	}

	Table, err := parse.getTableByTableID(TableId)
	if err != nil {
		*pos = StartPos + DataLen
		return err
	}

	PrimaryFields, err := parse.getPrimaryKey(Table)
	if err != nil {
		*pos = StartPos + DataLen
		return err
	}

	for _, v := range PrimaryFields {
		Column := parse.getColumnsByName(Table, v.ColumnName)
		// get the unique key.
		FiledLen, num, err := utils.MatchParseCompressed(data, *pos)
		if err != nil {
			return err
		}
		logs.Debug("FiledLen is ", FiledLen, "num is ", num)
		*pos += num
		value, err := utils.ParseData(
			Column.FieldType, Column.MySQLType,
			data[*pos:*pos+FiledLen], FiledLen,
			int(utils.GetFixedLength(Column.FieldType, Column.FieldLen)),
			Column.IsUnsigned, &Column.IsBinary)
		if err != nil {
			return err
		}

		logs.Debug("the table is ", Table.TableName, " table id is ", TableId, " unique value is ")
		v.ColumnValue = value

		*pos += FiledLen
	}

	var columns []*ibdata.Columns

	// Get update record.
	if UndoType == TRX_UNDO_UPD_EXIST_REC {
		// Get update column num.
		ColumnNum, num, err := utils.MatchParseCompressed(data, *pos)
		if err != nil {
			return err
		}

		logs.Debug("ColumnNum is ", ColumnNum)
		*pos += num

		var i uint64
		for i = 0; i < ColumnNum; i++ {
			Pos, num, err := utils.MatchParseCompressed(data, *pos)
			if err != nil {
				return err
			}

			logs.Debug("Pos is ", Pos, " num is ", num)
			*pos += num

			c, err := parse.getColumnByPos(Table, Pos)
			if err != nil {
				return err
			}

			Flen, num, err := utils.MatchParseCompressed(data, *pos)
			logs.Debug("Flen is ", Flen, " num is ", num, "Field Type is ", c.FieldType)
			*pos += num

			var value interface{}
			if *pos+Flen > uint64(len(data)) {
				value = nil
				Flen = 0
			} else {
				value, err = utils.ParseData(c.FieldType, c.MySQLType, data[*pos:], Flen,
					int(utils.GetFixedLength(c.FieldType, c.FieldLen)), c.IsUnsigned, &c.IsBinary)
				if err != nil {
					return err
				}

			}

			c.FieldValue = value
			columns = append(columns, c)
			*pos += Flen

			logs.Debug("values is ", value)
		}

		// Make data to sql statement, only for update statement.
		// Print sql statement when user identify the table name or the database name,
		// if user don't identify table and database name, print all sql statement.
		if Table.DBName == parse.DBName {
			if Table.TableName == parse.TableName {
				parse.makeSQL(Table, PrimaryFields, columns)
			} else if parse.TableName == "" {
				parse.makeSQL(Table, PrimaryFields, columns)
			}
		} else if parse.DBName == "" && parse.TableName == "" {
			parse.makeSQL(Table, PrimaryFields, columns)
		} else if parse.TableName != "" && parse.TableName == Table.TableName {
			parse.makeSQL(Table, PrimaryFields, columns)
		}
	}

	// TODO: confirm
	//if (UndoType != TRX_UNDO_UPD_EXIST_REC) || ((CmplInfo&1) == 0) {
	//	// get delete mark record.
	//	if int64(DataLen-(*pos-StartPos)) >= 5 {
	//		*pos += 2
	//		for {
	//			// TODO: uint64 don't have negative.
	//			if int64(DataLen-(*pos-StartPos)) <= 2 {
	//				logs.Debug("pos is", pos, "StartPos is ", StartPos, " len is ", DataLen)
	//				break
	//			}
	//
	//			Pos, num, err := utils.MatchParseCompressed(data, *pos)
	//			if err != nil {
	//				return err
	//			}
	//			logs.Debug("Pos is ", Pos)
	//			*pos += num
	//
	//			Flen, num, err := utils.MatchParseCompressed(data, *pos)
	//			logs.Debug("Flen is ", Flen)
	//			*pos += num
	//			*pos += Flen
	//		}
	//	}
	//}
	//
	//if int64(DataLen-(*pos-StartPos)) >= 2 {
	//	*pos += 2
	//}

	// if parse incorrectly, the pos may be wrong. add the data len directly.
	if *pos-StartPos != DataLen {
		*pos = StartPos + DataLen
	}

	logs.Debug("delta pos is ", *pos-StartPos,
		"undo insert len is ", DataLen, " complete undo insert parse.")

	return nil
}

func (parse *Parse) getPrimaryKey(Table ibdata.Tables) ([]*ibdata.Fields, error) {

	var fields []*ibdata.Fields

	for _, idx := range Table.Indexes {
		if strings.TrimSpace(idx.Name) == "PRIMARY" || strings.TrimSpace(idx.Name) == "GEN_CLUST_INDEX" {
			if strings.TrimSpace(idx.Name) == "GEN_CLUST_INDEX" {
				fields = []*ibdata.Fields{&ibdata.Fields{ColumnPos: 0, ColumnName: "DB_ROW_ID"}}
			} else {
				fields = idx.Fields
			}
		}
	}
	return fields, nil
}

func (parse *Parse) getColumnsByName(table ibdata.Tables, FieldName string) ibdata.Columns {
	for _, c := range table.Columns {
		if c.FieldName == FieldName {
			return c
		}
	}
	return ibdata.Columns{}
}

func (parse *Parse) getColumnByPos(Table ibdata.Tables, Pos uint64) (*ibdata.Columns, error) {
	for i, c := range Table.Columns {
		if uint64(i) == Pos {
			return &c, nil
		}
	}
	logs.Error("table ", Table.TableName, " have not found field pos ", Pos)
	return &ibdata.Columns{}, fmt.Errorf("field not found")
}

func (parse *Parse) getTableByTableID(TableID uint64) (ibdata.Tables, error) {
	v, ok := parse.TableMap[TableID]
	if ok {
		t := v
		return t, nil
	} else {
		ErrMsg := fmt.Sprintf("table not found, table id is %d", TableID)
		logs.Debug(ErrMsg)
		return ibdata.Tables{}, fmt.Errorf(ErrMsg)
	}
}

func (parse *Parse) MLOG_UNDO_INIT(data []byte, pos *uint64) error {
	_, num, err := utils.MatchParseCompressed(data, *pos)
	if err != nil {
		return err
	}
	*pos += num

	return nil
}

func (parse *Parse) MLOG_UNDO_HDR_REUSE(data []byte, pos *uint64) {
	utils.MatchUllReadComPressed(data, pos)
}

func (parse *Parse) MLOG_UNDO_HDR_CREATE(data []byte, pos *uint64) error {
	value, _, err := utils.MatchParseCompressed(data, *pos)
	if err != nil {
		logs.Error("parse MLOG_UNDO_HDR_CREATE failed, the error is ", err.Error())
	}

	size, err := utils.MatchGetCompressedSize(value)
	if err != nil {
		return err
	}

	*pos += size
	*pos += 4

	return nil
}

func (parse *Parse) MLOG_WRITE_STRING(data []byte, pos *uint64) {

	// Parses a log record written by mlog_write_string
	offset := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	len := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	*pos += len
	logs.Debug("offset is ", offset, " len is ", len)

}

func (parse *Parse) MLOG_FILE_OP(data []byte, pos *uint64, MyType uint64) {

	if MyType == MLOG_FILE_CREATE2 {
		flags := utils.MatchReadFrom4(data[*pos:])
		*pos += 4
		logs.Debug("flags is ", flags)
	}

	NameLen := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	*pos += NameLen

}

func (parse *Parse) MLOG_REC_MARK(data []byte, pos uint64) {
	pos += 2
}

func (parse *Parse) MLOG_REC_INSERT(data []byte, pos *uint64, mytype uint64) error {
	Pos, err := utils.ParseIndex(mytype == MLOG_COMP_REC_INSERT, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = Pos

		// page_cur_parse_insert_rec, Parses a log record of a record insert on a page.
		Pos, err := utils.ParseInsertRecord(false, data, *pos)

		if err != nil {
			return err
		} else {
			*pos = Pos
		}
	}

	return nil
}

func (parse *Parse) MLOG_REC_CLUST_DELETE_MARK(data []byte, pos *uint64, mytype uint64) error {

	Pos, err := utils.ParseIndex(mytype == MLOG_COMP_REC_CLUST_DELETE_MARK, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = Pos
		// btr_cur_parse_del_mark_set_clust_rec
		// Parses the redo log record for delete marking or unmarking of a clustered
		// index record.
		flags := utils.MatchReadFrom1(data[*pos:])
		*pos++

		value := utils.MatchReadFrom1(data[*pos:])
		*pos++

		Pos, num, err := utils.MatchParseCompressed(data, *pos)
		if err != nil {
			return err
		}

		logs.Debug("num is ", num)
		*pos += num

		RollPtr := utils.MatchReadFrom7(data[*pos:])
		*pos += 7

		// TODO: should use MatchUllParseComPressed.
		trxID := utils.MatchUllReadComPressed(data, pos)

		logs.Debug("flags is ", flags, "value is ", value, "Pos is ",
			Pos, "RollPtr is ", RollPtr, "TrxId is ", trxID)

		offset := utils.MatchReadFrom2(data[*pos:])
		*pos += 2

		logs.Debug("page offset is ", offset)

	}
	return nil
}

func (parse *Parse) MLOG_COMP_REC_SEC_DELETE_MARK(data []byte, pos *uint64) error {
	Pos, err := utils.ParseIndex(true, data, *pos)
	if err == nil {
		*pos = Pos
	}
	return err
}

func (parse *Parse) MLOG_REC_UPDATE_IN_PLACE(data []byte, pos *uint64, MyType uint64) error {

	// Catch panic
	defer func() {
		if err := recover(); err != nil {
			logs.Error("skip error, the error is ", err)
		}
	}()

	Pos, err := utils.ParseIndex(MyType == MLOG_COMP_REC_UPDATE_IN_PLACE, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = Pos
		// read flag.
		flag := utils.MatchReadFrom1(data[*pos:])
		*pos++

		// row_upd_parse_sys_vals Parses the log data of system field values.
		Pos, num, err := utils.MatchParseCompressed(data, *pos)
		if err != nil {
			return err
		}
		*pos += num
		if uint64(len(data)) < (*pos + 7) {
			ErrMsg := fmt.Sprintf("data is too short")
			logs.Debug(ErrMsg)
			return fmt.Errorf(ErrMsg)
		} else {
			RollPtr := utils.MatchReadFrom7(data[*pos:])
			// DATA_ROLL_PTR_LEN
			*pos += 7

			TrxId := utils.MatchUllReadComPressed(data, pos)

			logs.Debug("flag is ", flag, "pos is ",
				Pos, "RollPtr is ", RollPtr, "TrxId is ", TrxId)

			RecordOffset := utils.MatchReadFrom2(data[*pos:])
			*pos += 2

			// row_upd_index_parse Parses the log data written by row_upd_index_write_log.
			InfoBits := utils.MatchReadFrom1(data[*pos:])

			*pos++
			FieldsNum, num, err := utils.MatchParseCompressed(data, *pos)
			if err != nil {
				return err
			} else {
				logs.Debug("page offset is ", RecordOffset, " InfoBits is ",
					InfoBits, " FieldsNum is ", FieldsNum)
			}
			*pos += num

			var i uint64
			for i = 0; i < FieldsNum; i++ {
				FieldNo, num, err := utils.MatchParseCompressed(data, *pos)
				if err != nil {
					return err
				}
				*pos += num

				FieldLen, num, err := utils.MatchParseCompressed(data, *pos)
				if err != nil {
					return err
				} else {
					logs.Debug("FieldNo is ", FieldNo, " FieldLen is ", FieldLen)
				}
				*pos += num
				*pos += FieldLen
			}
		}
	}
	return nil
}

func (parse *Parse) MLOG_REC_DELETE(data []byte, pos *uint64, MyType uint64) error {
	Pos, err := utils.ParseIndex(MyType == MLOG_COMP_REC_DELETE, data, *pos)
	if err == nil {
		*pos = Pos

		// page_cur_parse_delete_rec
		// Parses log record of a record delete on a page.
		offset := utils.MatchReadFrom2(data[*pos:])
		*pos += 2

		logs.Debug("offset is ", offset)

	}
	return err
}

func (parse *Parse) MLOG_LIST_DELETE(data []byte, pos *uint64, MyType uint64) error {
	Pos, err := utils.ParseIndex(MyType == MLOG_COMP_LIST_START_DELETE ||
		MyType == MLOG_COMP_LIST_END_DELETE, data, *pos)
	if err == nil {
		*pos = Pos

		// page_parse_delete_rec_list
		// Parses a log record of a record list end or start deletion.

		offset := utils.MatchReadFrom2(data[*pos:])
		*pos += 2

		logs.Debug("offset is ", offset)
	}
	return err
}

func (parse *Parse) MLOG_LIST_END_COPY_CREATED(data []byte, pos *uint64, MyType uint64) error {

	Pos, err := utils.ParseIndex(MyType == MLOG_COMP_LIST_END_COPY_CREATED, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = Pos

		// page_parse_copy_rec_list_to_created_page
		// Parses a log record of copying a record list end to a new created page.
		LogDataLen := utils.MatchReadFrom4(data[*pos:])
		*pos += 4

		RecEnd := *pos + LogDataLen

		if RecEnd > uint64(len(data)) {
			ErrMsg := fmt.Sprintf("data too short")
			logs.Debug(ErrMsg)
			return fmt.Errorf(ErrMsg)
		}

		logs.Debug("LogDataLen is ", LogDataLen)

		for {
			if *pos < RecEnd {

				logs.Debug("LogDataLen is ", LogDataLen)

				// page_cur_parse_insert_rec, Parses a log record of a record insert on a page.
				Pos, err := utils.ParseInsertRecord(true, data, *pos)
				if err != nil {
					return err
				} else {
					*pos = Pos
				}

			} else {
				break
			}

		}
	}
	return nil
}

func (parse *Parse) MLOG_PAGE_REORGANIZE(data []byte, pos *uint64, MyType uint64) error {

	Pos, err := utils.ParseIndex(MyType != MLOG_PAGE_REORGANIZE, data, *pos)
	if err == nil {
		*pos = Pos

		if MyType == MLOG_ZIP_PAGE_REORGANIZE {
			level := utils.MatchReadFrom1(data[*pos:])
			*pos++
			logs.Debug("level is ", level)
		}
	}
	return err
}

func (parse *Parse) MLOG_ZIP_WRITE_NODE_PTR(data []byte, pos *uint64) {

	// TODO: confirm.

	offset := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	Zoffset := utils.MatchReadFrom2(data[*pos:])
	*pos += 2

	logs.Debug("offset is", offset, "Zoffset is ", Zoffset)

	// REC_NODE_PTR_SIZE
	*pos += 4
}

func (parse *Parse) MLOG_ZIP_WRITE_BLOB_PTR(data []byte, pos uint64) {
	pos += 24
}

func (parse *Parse) MLOG_ZIP_WRITE_HEADER(data []byte, pos *uint64) {
	// TODO: confirm.

	offset := uint64(data[*pos:][0])
	logs.Debug("offset is ", offset)

	*pos++
	dataLen := uint64(data[*pos:][0])
	*pos++

	*pos += dataLen
}

func (parse *Parse) MLOG_ZIP_PAGE_COMPRESS(data []byte, pos *uint64) {
	size := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	TrailerSize := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	*pos += 8 + size + TrailerSize

	logs.Debug("size is:", size, "TrailerSize is :", TrailerSize)
}

func (parse *Parse) MLOG_ZIP_PAGE_COMPRESS_NO_DATA(data []byte, pos *uint64) error {
	// TODO: confirm.

	Pos, err := utils.ParseIndex(true, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = Pos
		level := utils.MatchReadFrom1(data[*pos:])
		logs.Debug("level is ", level)
		*pos++
	}
	return nil
}
