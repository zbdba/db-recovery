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

// MLOG_N_BYTES ...
func (parse *Parse) mlogNBytes(data []byte, pos *uint64, myType uint64) error {

	offset := utils.MatchReadFrom2(data[*pos:])
	logs.Debug("offset is", offset, "pos is ", *pos, " data len is ", len(data))

	*pos += 2

	if myType == MLOG_8BYTES {
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

// MLOG_REC_SEC_DELETE_MARK ...
func (parse *Parse) mlogRecSecDeleteMark(data []byte, pos *uint64) {

	value := utils.MatchReadFrom1(data[*pos:])
	*pos++

	offset := utils.MatchReadFrom2(data[*pos:])
	*pos += 2

	logs.Debug("values is ", value, " offset is ", offset)
}

// Parse the undo record.

// MLOG_UNDO_INSERT ...
func (parse *Parse) mlogUndoInsert(data []byte, pos *uint64) error {

	dataLen := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	startPos := *pos

	// catch panic
	defer func() {
		if err := recover(); err != nil {
			logs.Error("skip error, the error is ", err)
			*pos = startPos + dataLen
		}
	}()

	typeCmpl := int64(utils.MatchReadFrom1(data[*pos:]))
	*pos++

	typeCmpl &= ^TRX_UNDO_UPD_EXTERN
	undoType := typeCmpl & (TRX_UNDO_CMPL_INFO_MULT - 1)
	cmplInfo := typeCmpl

	logs.Debug("undoType is", undoType, "cmplInfo is ", cmplInfo)

	undoNo, err := utils.MatchUllReadMuchCompressed(data[*pos:])
	if err != nil {
		*pos = startPos + dataLen
		return err
	}
	logs.Debug("undoNo is ", undoNo)

	*pos += utils.MachUllGetMuchCompressedSize(undoNo)

	tableID, err := utils.MatchUllReadMuchCompressed(data[*pos:])
	if err != nil {
		*pos = startPos + dataLen
		return err
	}

	logs.Debug("tableID is ", tableID)
	*pos += utils.MachUllGetMuchCompressedSize(tableID)

	if undoType != TRX_UNDO_INSERT_REC {
		// Skip info bits
		logs.Debug("info bits is ", data[*pos:][0])
		*pos++

		trxID := utils.MatchUllReadComPressed(data, pos)
		logs.Debug("TrxID is ", trxID)

		rollPtr := utils.MatchUllReadComPressed(data, pos)
		logs.Debug("rollPtr is ", rollPtr)
	}

	Table, err := parse.getTableByTableID(tableID)
	if err != nil {
		*pos = startPos + dataLen
		return err
	}

	primaryFields, err := parse.getPrimaryKey(Table)
	if err != nil {
		*pos = startPos + dataLen
		return err
	}

	for _, v := range primaryFields {
		column := parse.getColumnsByName(Table, v.ColumnName)
		// get the unique key.
		FiledLen, num, err := utils.MatchParseCompressed(data, *pos)
		if err != nil {
			return err
		}
		logs.Debug("FiledLen is ", FiledLen, "num is ", num)
		*pos += num
		value, err := utils.ParseData(
			column.FieldType, column.MySQLType,
			data[*pos:*pos+FiledLen], FiledLen,
			int(utils.GetFixedLength(column.FieldType, column.FieldLen)),
			column.IsUnsigned, &column.IsBinary)
		if err != nil {
			return err
		}

		logs.Debug("the table is ", Table.TableName, " table id is ", tableID, " unique value is ")
		v.ColumnValue = value

		*pos += FiledLen
	}

	var columns []*ibdata.Columns

	// Get update record.
	if undoType == TRX_UNDO_UPD_EXIST_REC {
		// Get update column num.
		columnNum, num, err := utils.MatchParseCompressed(data, *pos)
		if err != nil {
			return err
		}

		logs.Debug("columnNum is ", columnNum)
		*pos += num

		var i uint64
		for i = 0; i < columnNum; i++ {
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

			fLen, num, err := utils.MatchParseCompressed(data, *pos)
			logs.Debug("fLen is ", fLen, " num is ", num, "Field Type is ", c.FieldType)
			*pos += num

			var value interface{}
			if *pos+fLen > uint64(len(data)) {
				value = nil
				fLen = 0
			} else {
				value, err = utils.ParseData(c.FieldType, c.MySQLType, data[*pos:], fLen,
					int(utils.GetFixedLength(c.FieldType, c.FieldLen)), c.IsUnsigned, &c.IsBinary)
				if err != nil {
					return err
				}

			}

			c.FieldValue = value
			columns = append(columns, c)
			*pos += fLen

			logs.Debug("values is ", value)
		}

		// Make data to sql statement, only for update statement.
		// Print sql statement when user identify the table name or the database name,
		// if user don't identify table and database name, print all sql statement.
		if Table.DBName == parse.DBName {
			if Table.TableName == parse.TableName {
				parse.makeSQL(Table, primaryFields, columns)
			} else if parse.TableName == "" {
				parse.makeSQL(Table, primaryFields, columns)
			}
		} else if parse.DBName == "" && parse.TableName == "" {
			parse.makeSQL(Table, primaryFields, columns)
		} else if parse.TableName != "" && parse.TableName == Table.TableName {
			parse.makeSQL(Table, primaryFields, columns)
		}
	}

	// TODO: confirm
	//if (undoType != TRX_UNDO_UPD_EXIST_REC) || ((cmplInfo&1) == 0) {
	//	// get delete mark record.
	//	if int64(dataLen-(*pos-startPos)) >= 5 {
	//		*pos += 2
	//		for {
	//			// TODO: uint64 don't have negative.
	//			if int64(dataLen-(*pos-startPos)) <= 2 {
	//				logs.Debug("pos is", pos, "startPos is ", startPos, " len is ", dataLen)
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
	//			fLen, num, err := utils.MatchParseCompressed(data, *pos)
	//			logs.Debug("fLen is ", fLen)
	//			*pos += num
	//			*pos += fLen
	//		}
	//	}
	//}
	//
	//if int64(dataLen-(*pos-startPos)) >= 2 {
	//	*pos += 2
	//}

	// if parse incorrectly, the pos may be wrong. add the data len directly.
	if *pos-startPos != dataLen {
		*pos = startPos + dataLen
	}

	logs.Debug("delta pos is ", *pos-startPos,
		"undo insert len is ", dataLen, " complete undo insert parse.")

	return nil
}

func (parse *Parse) getPrimaryKey(table ibdata.Tables) ([]*ibdata.Fields, error) {

	var fields []*ibdata.Fields

	for _, idx := range table.Indexes {
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

func (parse *Parse) getColumnsByName(table ibdata.Tables, fieldName string) ibdata.Columns {
	for _, c := range table.Columns {
		if c.FieldName == fieldName {
			return c
		}
	}
	return ibdata.Columns{}
}

func (parse *Parse) getColumnByPos(table ibdata.Tables, pos uint64) (*ibdata.Columns, error) {
	for i, c := range table.Columns {
		if uint64(i) == pos {
			return &c, nil
		}
	}
	logs.Error("table ", table.TableName, " have not found field pos ", pos)
	return &ibdata.Columns{}, fmt.Errorf("field not found")
}

func (parse *Parse) getTableByTableID(tableID uint64) (ibdata.Tables, error) {
	v, ok := parse.TableMap[tableID]
	if ok {
		t := v
		return t, nil
	} else {
		ErrMsg := fmt.Sprintf("table not found, table id is %d", tableID)
		logs.Debug(ErrMsg)
		return ibdata.Tables{}, fmt.Errorf(ErrMsg)
	}
}

// MLOG_UNDO_INIT ...
func (parse *Parse) mlogUndoInit(data []byte, pos *uint64) error {
	_, num, err := utils.MatchParseCompressed(data, *pos)
	if err != nil {
		return err
	}
	*pos += num

	return nil
}

// MLOG_UNDO_HDR_REUSE ...
func (parse *Parse) mlogUndoHdrReuse(data []byte, pos *uint64) {
	utils.MatchUllReadComPressed(data, pos)
}

// MLOG_UNDO_HDR_CREATE ...
func (parse *Parse) mlogUndoHdrCreate(data []byte, pos *uint64) error {
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

// MLOG_WRITE_STRING ...
func (parse *Parse) mlogWriteString(data []byte, pos *uint64) {

	// Parses a log record written by mlog_write_string
	offset := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	len := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	*pos += len
	logs.Debug("offset is ", offset, " len is ", len)

}

// MLOG_FILE_OP ...
func (parse *Parse) mlogFileOp(data []byte, pos *uint64, myType uint64) {

	if myType == MLOG_FILE_CREATE2 {
		flags := utils.MatchReadFrom4(data[*pos:])
		*pos += 4
		logs.Debug("flags is ", flags)
	}

	nameLen := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	*pos += nameLen

}

// MLOG_REC_MARK ...
func (parse *Parse) mlogRecMark(data []byte, pos uint64) {
	pos += 2
}

// MLOG_REC_INSERT ...
func (parse *Parse) mlogRecInsert(data []byte, pos *uint64, myType uint64) error {
	position, err := utils.ParseIndex(myType == MLOG_COMP_REC_INSERT, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = position

		// page_cur_parse_insert_rec, Parses a log record of a record insert on a page.
		position, err = utils.ParseInsertRecord(false, data, *pos)

		if err != nil {
			return err
		} else {
			*pos = position
		}
	}

	return nil
}

// MLOG_REC_CLUST_DELETE_MARK ...
func (parse *Parse) mlogRecClustDeleteMark(data []byte, pos *uint64, myType uint64) error {

	position, err := utils.ParseIndex(myType == MLOG_COMP_REC_CLUST_DELETE_MARK, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = position
		// btr_cur_parse_del_mark_set_clust_rec
		// Parses the redo log record for delete marking or unMarking of a clustered
		// index record.
		flags := utils.MatchReadFrom1(data[*pos:])
		*pos++

		value := utils.MatchReadFrom1(data[*pos:])
		*pos++

		position, num, err := utils.MatchParseCompressed(data, *pos)
		if err != nil {
			return err
		}

		logs.Debug("num is ", num)
		*pos += num

		rollPtr := utils.MatchReadFrom7(data[*pos:])
		*pos += 7

		// TODO: should use MatchUllParseComPressed.
		trxID := utils.MatchUllReadComPressed(data, pos)

		logs.Debug("flags is ", flags, "value is ", value, "position is ",
			position, "rollPtr is ", rollPtr, "TrxId is ", trxID)

		offset := utils.MatchReadFrom2(data[*pos:])
		*pos += 2

		logs.Debug("page offset is ", offset)

	}
	return nil
}

// MLOG_COMP_REC_SEC_DELETE_MARK ...
func (parse *Parse) mlogCompRecSecDeleteMark(data []byte, pos *uint64) error {
	position, err := utils.ParseIndex(true, data, *pos)
	if err == nil {
		*pos = position
	}
	return err
}

// MLOG_REC_UPDATE_IN_PLACE ...
func (parse *Parse) mlogRecUpdateInPlace(data []byte, pos *uint64, MyType uint64) error {

	// Catch panic
	defer func() {
		if err := recover(); err != nil {
			logs.Error("skip error, the error is ", err)
		}
	}()

	position, err := utils.ParseIndex(MyType == MLOG_COMP_REC_UPDATE_IN_PLACE, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = position
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
			rollPtr := utils.MatchReadFrom7(data[*pos:])
			// DATA_ROLL_PTR_LEN
			*pos += 7

			trxID := utils.MatchUllReadComPressed(data, pos)

			logs.Debug("flag is ", flag, "pos is ", Pos, "rollPtr is ", rollPtr, "trxID is ", trxID)

			recordOffset := utils.MatchReadFrom2(data[*pos:])
			*pos += 2

			// row_upd_index_parse Parses the log data written by row_upd_index_write_log.
			InfoBits := utils.MatchReadFrom1(data[*pos:])

			*pos++
			FieldsNum, num, err := utils.MatchParseCompressed(data, *pos)
			if err != nil {
				return err
			} else {
				logs.Debug("page offset is ", recordOffset, " InfoBits is ",
					InfoBits, " FieldsNum is ", FieldsNum)
			}
			*pos += num

			var i uint64
			for i = 0; i < FieldsNum; i++ {
				fieldNo, num, err := utils.MatchParseCompressed(data, *pos)
				if err != nil {
					return err
				}
				*pos += num

				fieldLen, num, err := utils.MatchParseCompressed(data, *pos)
				if err != nil {
					return err
				} else {
					logs.Debug("fieldNo is ", fieldNo, " fieldLen is ", fieldLen)
				}
				*pos += num
				*pos += fieldLen
			}
		}
	}
	return nil
}

// MLOG_REC_DELETE ...
func (parse *Parse) mlogRecDelete(data []byte, pos *uint64, MyType uint64) error {
	position, err := utils.ParseIndex(MyType == MLOG_COMP_REC_DELETE, data, *pos)
	if err == nil {
		*pos = position

		// page_cur_parse_delete_rec
		// Parses log record of a record delete on a page.
		offset := utils.MatchReadFrom2(data[*pos:])
		*pos += 2

		logs.Debug("offset is ", offset)

	}
	return err
}

// MLOG_LIST_DELETE ...
func (parse *Parse) mlogListDelete(data []byte, pos *uint64, myType uint64) error {
	position, err := utils.ParseIndex(myType == MLOG_COMP_LIST_START_DELETE ||
		myType == MLOG_COMP_LIST_END_DELETE, data, *pos)
	if err == nil {
		*pos = position

		// page_parse_delete_rec_list
		// Parses a log record of a record list end or start deletion.

		offset := utils.MatchReadFrom2(data[*pos:])
		*pos += 2

		logs.Debug("offset is ", offset)
	}
	return err
}

// MLOG_LIST_END_COPY_CREATED ...
func (parse *Parse) mlogListEndCopyCreated(data []byte, pos *uint64, myType uint64) error {

	position, err := utils.ParseIndex(myType == MLOG_COMP_LIST_END_COPY_CREATED, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = position

		// page_parse_copy_rec_list_to_created_page
		// Parses a log record of copying a record list end to a new created page.
		logDataLen := utils.MatchReadFrom4(data[*pos:])
		*pos += 4

		recEnd := *pos + logDataLen

		if recEnd > uint64(len(data)) {
			ErrMsg := fmt.Sprintf("data too short")
			logs.Debug(ErrMsg)
			return fmt.Errorf(ErrMsg)
		}

		logs.Debug("logDataLen is ", logDataLen)

		for {
			if *pos < recEnd {

				logs.Debug("logDataLen is ", logDataLen)

				// page_cur_parse_insert_rec, Parses a log record of a record insert on a page.
				position, err := utils.ParseInsertRecord(true, data, *pos)
				if err != nil {
					return err
				} else {
					*pos = position
				}

			} else {
				break
			}

		}
	}
	return nil
}

// MLOG_PAGE_REORGANIZE ...
func (parse *Parse) mlogPageReorganize(data []byte, pos *uint64, myType uint64) error {

	position, err := utils.ParseIndex(myType != MLOG_PAGE_REORGANIZE, data, *pos)
	if err == nil {
		*pos = position

		if myType == MLOG_ZIP_PAGE_REORGANIZE {
			level := utils.MatchReadFrom1(data[*pos:])
			*pos++
			logs.Debug("level is ", level)
		}
	}
	return err
}

// MLOG_ZIP_WRITE_NODE_PTR ...
func (parse *Parse) mlogZipWriteNodePtr(data []byte, pos *uint64) {

	// TODO: confirm.

	offset := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	zOffset := utils.MatchReadFrom2(data[*pos:])
	*pos += 2

	logs.Debug("offset is", offset, "zOffset is ", zOffset)

	// REC_NODE_PTR_SIZE
	*pos += 4
}

// MLOG_ZIP_WRITE_BLOB_PTR ...
func (parse *Parse) mlogZipWriteBlobPtr(data []byte, pos uint64) {
	pos += 24
}

// MLOG_ZIP_WRITE_HEADER ...
func (parse *Parse) mlogZipWriteHeader(data []byte, pos *uint64) {
	// TODO: confirm.

	offset := uint64(data[*pos:][0])
	logs.Debug("offset is ", offset)

	*pos++
	dataLen := uint64(data[*pos:][0])
	*pos++

	*pos += dataLen
}

// MLOG_ZIP_PAGE_COMPRESS ...
func (parse *Parse) mlogZipPageCompress(data []byte, pos *uint64) {
	size := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	trailerSize := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	*pos += 8 + size + trailerSize

	logs.Debug("size is:", size, "trailerSize is :", trailerSize)
}

// MLOG_ZIP_PAGE_COMPRESS_NO_DATA ...
func (parse *Parse) mlogZipPageCompressNoData(data []byte, pos *uint64) error {
	// TODO: confirm.

	position, err := utils.ParseIndex(true, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = position
		level := utils.MatchReadFrom1(data[*pos:])
		logs.Debug("level is ", level)
		*pos++
	}
	return nil
}
