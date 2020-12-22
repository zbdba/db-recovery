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
	"io"
	"os"
	"strings"

	"github.com/zbdba/db-recovery/recovery/ibdata"
	"github.com/zbdba/db-recovery/recovery/utils"
	"github.com/zbdba/db-recovery/recovery/utils/logs"
)

type ParseRedo struct {
	// Store table info.
	TableMap map[uint64]ibdata.Tables

	// The table name which you want to recovery.
	TableName string

	// The database name which you want to recovery.
	DBName string
}

func NewParseRedo(IbFilePath string, TableName string, DBName string) (*ParseRedo, error) {
	p := &ParseRedo{TableName: TableName, DBName: DBName}

	// get data dict
	I := ibdata.NewParseIB()
	ParseDictErr := I.ParseDictPage(IbFilePath)
	if ParseDictErr != nil {
		return nil, ParseDictErr
	}
	p.TableMap = I.TableMap

	return p, nil
}

// Parse the redo log file
// The redo log file have four parts:
// 1.redo log header
// 2.checkpoint 1
// 3.checkpoint 2
// 4.redo block
// And every parts is 512 bytes, there will be many
// redo blocks which store the redo record.
func (parse *ParseRedo) Parse(LogFileList []string) error {
	var data []byte
	for _, LogFile := range LogFileList {
		file, err := os.Open(LogFile)
		if err != nil {
			logs.Error("Error while opening file, the error is ", err)
		}

		defer file.Close()

		// Parse read redo log header
		ReadHaderErr := parse.ReadHeader(file)
		if ReadHaderErr != nil {
			return ReadHaderErr
		}

		// Parse read redo log checkpoint
		ReadCheckPointErr := parse.ReadCheckpoint(file)
		if ReadCheckPointErr != nil {
			return ReadCheckPointErr
		}

		// Move to the start of the logs
		// Current position is 512 + 512 + 512 = 1536 and logs start at 2048
		if pos, err := file.Seek(BlockSize, io.SeekCurrent); err == nil {
			logs.Debug("Current position: %d", pos)
		}

		for {
			// When parsing the redo log file, record the read position.
			var pos uint64
			d, err := utils.ReadNextBytes(file, BlockSize)
			if err != nil {
				break
			}

			// Parse the redo block header.
			// The redo block consists of redo log header and redo log data.
			DataLen, FirstRecord, ReadErr := parse.ReadRedoBlockHeader(&pos, d)
			if ReadErr != nil || DataLen == 0 {
				break
			}

			// LOG_BLOCK_TRL_SIZE, for checksum.
			if DataLen >= BlockSize {
				// TODO: const
				DataLen -= 4
			}

			// Sometimes the first block may not be the beginning of the log record,
			// it may be the end of the previous log record. we will make an error
			// when we parse directly. At this time, we will start parsing directly
			// from the position specified by first record. If first record
			// is equal to 0, this block is used by all previous log records, we just skip it.
			if len(data) == 0 && FirstRecord == uint64(0) {
				continue
			}

			if len(data) == 0 && FirstRecord > 12 {
				pos += FirstRecord - 12
			}

			// Add all redo block data.
			data = append(data, d[pos:DataLen]...)
		}
	}

	// Start parse redo log data.
	ParseDataErr := parse.ParseRedoBlockData(data)
	if ParseDataErr != nil {
		logs.Debug("parse redo block data failed, the error is ", ParseDataErr)
		return ParseDataErr
	}
	return nil
}

// Parse the redo block data, it consists of many redo records.
// There are about 55 redo log type, and every redo log type have different data.
// We just want get the MLOG_UNDO_INSERT type redo record which store the undo info.
// But due to the design of redo, we must parse out each type in order.
func (parse *ParseRedo) ParseRedoBlockData(data []byte) error {
	var pos uint64
	for {
		if (int64(len(data)) - int64(pos)) < 5 {
			break
		}
		if (uint64(len(data)) - pos) < 5 {
			break
		}

		// Read initial log record.
		LogType := utils.MatchReadFrom1(data[pos:])
		pos++
		StartPos := pos

		if LogType == MLOG_MULTI_REC_END {
			// don't parse MLOG_MULTI_REC_END type redo log.
			continue
		}

		// Get redo log type.
		MLOG_SINGLE_REC_FLAG := 128
		LogType = uint64(int(LogType) & ^MLOG_SINGLE_REC_FLAG)

		if LogType != MLOG_CHECKPOINT {

			// Get space id
			SpaceID, num, err := utils.MatchParseCompressed(data, pos)
			pos += num
			if err != nil {
				logs.Error("get log record space id failed, the error is ", err)
				return err

			}
			// Get the page no
			PageNo, num, err := utils.MatchParseCompressed(data, pos)
			pos += num
			if err != nil {
				logs.Error("get log record page number failed, the error is ", err)
				return err
			}

			if !parse.ValidateLogHeader(LogType, SpaceID) {
				pos = StartPos
				continue
			}

			logs.Debug("LogType is:", LogType, "SpaceID is:", SpaceID, "PageNo is:", PageNo)

			Table, err := parse.GetTableBySpaceID(SpaceID)
			if err != nil {
				logs.Error("can't find table, space id is ", SpaceID)
			} else {
				logs.Debug("Table name is ", Table.TableName)
			}
		} else {
			logs.Debug("LogType is:", LogType)
		}

		switch LogType {

		case MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES, MLOG_8BYTES:
			logs.Debug("start parse MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES, MLOG_8BYTES log record")
			err := parse.MLOG_N_BYTES(data, &pos, LogType)
			if err != nil {
				break
			}

		case MLOG_REC_SEC_DELETE_MARK:
			logs.Debug("start parse MLOG_REC_SEC_DELETE_MARK log record")
			parse.MLOG_REC_SEC_DELETE_MARK(data, &pos)

		case MLOG_UNDO_INSERT:
			logs.Debug("start parse MLOG_UNDO_INSERT log record")
			err := parse.MLOG_UNDO_INSERT(data, &pos)
			if err != nil {
				break
			}

		case MLOG_UNDO_ERASE_END:
			logs.Debug("start parse MLOG_UNDO_ERASE_END log record")
			continue

		case MLOG_UNDO_INIT:
			logs.Debug("start parse MLOG_UNDO_INIT log record")
			err := parse.MLOG_UNDO_INIT(data, &pos)
			if err != nil {
				break
			}
		case MLOG_UNDO_HDR_DISCARD:
			logs.Debug("start parse MLOG_UNDO_HDR_DISCARD log record")
			continue

		case MLOG_UNDO_HDR_REUSE:
			logs.Debug("start parse MLOG_UNDO_HDR_REUSE log record")
			parse.MLOG_UNDO_HDR_REUSE(data, &pos)

		case MLOG_UNDO_HDR_CREATE:
			logs.Debug("start parse MLOG_UNDO_HDR_CREATE log record")
			err := parse.MLOG_UNDO_HDR_CREATE(data, &pos)
			if err != nil {
				break
			}

		case MLOG_IBUF_BITMAP_INIT:
			logs.Debug("start parse MLOG_IBUF_BITMAP_INIT log record")
			continue

		case MLOG_INIT_FILE_PAGE, MLOG_INIT_FILE_PAGE2:
			logs.Debug("start parse MLOG_INIT_FILE_PAGE or " +
				"MLOG_INIT_FILE_PAGE2 log record")
			continue

		case MLOG_WRITE_STRING:
			logs.Debug("start parse MLOG_WRITE_STRING log record")
			parse.MLOG_WRITE_STRING(data, &pos)

		case MLOG_MULTI_REC_END:
			logs.Debug("start parse MLOG_MULTI_REC_END log record")
			//continue

		case MLOG_DUMMY_RECORD:
			logs.Debug("start parse MLOG_DUMMY_RECORD log record")
			continue

		case MLOG_FILE_RENAME, MLOG_FILE_CREATE,
			MLOG_FILE_DELETE, MLOG_FILE_CREATE2,
			MLOG_FILE_RENAME2, MLOG_FILE_NAME:
			logs.Debug("start parse MLOG_FILE_RENAME, MLOG_FILE_CREATE, " +
				"MLOG_FILE_DELETE, MLOG_FILE_CREATE2 log record")
			parse.MLOG_FILE_OP(data, &pos, LogType)

		case MLOG_REC_MIN_MARK, MLOG_COMP_REC_MIN_MARK:
			logs.Debug("start parse MLOG_REC_MIN_MARK, MLOG_COMP_REC_MIN_MARK log record")
			pos += 2

		case MLOG_PAGE_CREATE, MLOG_COMP_PAGE_CREATE:
			logs.Debug("start parse MLOG_PAGE_CREATE, MLOG_COMP_PAGE_CREATE log record")
			continue

		case MLOG_REC_INSERT, MLOG_COMP_REC_INSERT:
			logs.Debug("start parse MLOG_REC_INSERT, MLOG_COMP_REC_INSERT log record")
			err := parse.MLOG_REC_INSERT(data, &pos, LogType)
			if err != nil {
				return err
			}

		case MLOG_REC_CLUST_DELETE_MARK, MLOG_COMP_REC_CLUST_DELETE_MARK:
			logs.Debug("start parse MLOG_REC_CLUST_DELETE_MARK, MLOG_COMP_REC_CLUST_DELETE_MARK log record")
			err := parse.MLOG_REC_CLUST_DELETE_MARK(data, &pos, LogType)
			if err != nil {
				break
			}

		case MLOG_COMP_REC_SEC_DELETE_MARK:
			logs.Debug("start parse MLOG_COMP_REC_SEC_DELETE_MARK log record")
			err := parse.MLOG_COMP_REC_SEC_DELETE_MARK(data, &pos)
			if err != nil {
				break
			}

		case MLOG_REC_UPDATE_IN_PLACE, MLOG_COMP_REC_UPDATE_IN_PLACE:
			logs.Debug("start parse MLOG_REC_UPDATE_IN_PLACE, MLOG_COMP_REC_UPDATE_IN_PLACE log record")
			err := parse.MLOG_REC_UPDATE_IN_PLACE(data, &pos, LogType)
			if err != nil {
				break
			}

		case MLOG_REC_DELETE, MLOG_COMP_REC_DELETE:
			logs.Debug("start parse MLOG_REC_DELETE, MLOG_COMP_REC_DELETE log record")
			err := parse.MLOG_REC_DELETE(data, &pos, LogType)
			if err != nil {
				break
			}

		case MLOG_LIST_END_DELETE,
			MLOG_COMP_LIST_END_DELETE,
			MLOG_LIST_START_DELETE,
			MLOG_COMP_LIST_START_DELETE:
			logs.Debug("start parse MLOG_LIST_END_DELETE,MLOG_COMP_LIST_END_DELETE," +
				"MLOG_LIST_START_DELETE,MLOG_COMP_LIST_START_DELETE log record")
			err := parse.MLOG_LIST_DELETE(data, &pos, LogType)
			if err != nil {
				break
			}

		case MLOG_LIST_END_COPY_CREATED, MLOG_COMP_LIST_END_COPY_CREATED:
			logs.Debug("start parse MLOG_LIST_END_COPY_CREATED, MLOG_COMP_LIST_END_COPY_CREATED log record")
			err := parse.MLOG_LIST_END_COPY_CREATED(data, &pos, LogType)
			if err != nil {
				break
			}

		case MLOG_PAGE_REORGANIZE, MLOG_COMP_PAGE_REORGANIZE, MLOG_ZIP_PAGE_REORGANIZE:
			logs.Debug("start parse MLOG_PAGE_REORGANIZE, MLOG_COMP_PAGE_REORGANIZE, " +
				"MLOG_ZIP_PAGE_REORGANIZE log record")
			err := parse.MLOG_PAGE_REORGANIZE(data, &pos, LogType)
			if err != nil {
				break
			}

		case MLOG_ZIP_WRITE_NODE_PTR:
			logs.Debug("start parse MLOG_ZIP_WRITE_NODE_PTR log record")
			parse.MLOG_ZIP_WRITE_NODE_PTR(data, &pos)

		case MLOG_ZIP_WRITE_BLOB_PTR:
			logs.Debug("start parse MLOG_ZIP_WRITE_BLOB_PTR log record")
			pos += 24

		case MLOG_ZIP_WRITE_HEADER:
			logs.Debug("start parse MLOG_ZIP_WRITE_HEADER log record")
			parse.MLOG_ZIP_WRITE_HEADER(data, &pos)

		case MLOG_ZIP_PAGE_COMPRESS:
			logs.Debug("start parse MLOG_ZIP_PAGE_COMPRESS log record")
			parse.MLOG_ZIP_PAGE_COMPRESS(data, &pos)

		case MLOG_ZIP_PAGE_COMPRESS_NO_DATA:
			logs.Debug("start parse MLOG_ZIP_PAGE_COMPRESS_NO_DATA log record")
			err := parse.MLOG_ZIP_PAGE_COMPRESS_NO_DATA(data, &pos)
			if err != nil {
				break
			}

		case MLOG_CHECKPOINT:
			logs.Debug("start prase MLOG_CHECKPOINT")
			pos += 8
			break
		case MLOG_COMP_PAGE_CREATE_RTREE, MLOG_PAGE_CREATE_RTREE:
			logs.Debug("start prase MLOG_COMP_PAGE_CREATE_RTREE " +
				"or MLOG_PAGE_CREATE_RTREE")
			break
		case MLOG_TRUNCATE:
			logs.Debug("start prase MLOG_TRUNCATE")
			pos += 8
			break
		case MLOG_INDEX_LOAD:
			logs.Debug("start prase MLOG_INDEX_LOAD")
			pos += 8
			break
		default:
			logs.Debug("unkown rMLOG_REC_UPDATE_IN_PLACEedo type, break.")
			break
		}
	}
	return nil
}

// Parse the redo block header.
func (parse *ParseRedo) ReadRedoBlockHeader(pos *uint64, d []byte) (uint64, uint64, error) {

	LogBlockNo := utils.MatchReadFrom4(d)
	*pos += 4

	DataLen := utils.MatchReadFrom2(d[*pos:])
	*pos += 2

	FirstRecord := utils.MatchReadFrom2(d[*pos:])
	*pos += 2

	CheckpointNo := utils.MatchReadFrom4(d[*pos:])
	*pos += 4

	logs.Debug("==================================")
	logs.Debug("LogBlockNo:", LogBlockNo)
	logs.Debug("DataLen:", DataLen)
	logs.Debug("FirstRecord:", FirstRecord)
	logs.Debug("CheckpointNo:", CheckpointNo)
	logs.Debug("==================================")
	logs.Debug("")

	return DataLen, FirstRecord, nil
}

func (parse *ParseRedo) ReadHeader(file *os.File) error {

	pos := 0
	data, err1 := utils.ReadNextBytes(file, 512)
	if err1 != nil {
		return err1
	}

	LOG_HEADER_FORMAT := utils.MatchReadFrom4(data[pos:])
	logs.Debug("LOG_HEADER_FORMAT is ", LOG_HEADER_FORMAT)
	pos += 8

	LOG_HEADER_START_LSN := utils.MatchReadFrom8(data[pos:])
	logs.Debug("LOG_HEADER_START_LSN is ", LOG_HEADER_START_LSN)

	return nil
}

func (parse *ParseRedo) ReadCheckpoint(file *os.File) error {

	checkpoint := Checkpoint{}
	const cpsize = 512
	err := utils.ReadIntoStruct(file, &checkpoint, cpsize)
	if err != nil {
		return err
	}
	//fmt.Println("================================================================================")
	//fmt.Println("Parsed first checkpoint data:")
	//fmt.Printf("Number     : 0x%X\n", checkpoint.Number)
	//fmt.Printf("LSN        : 0x%X\n", checkpoint.LSN)
	//fmt.Printf("Offset     : 0x%d\n", checkpoint.Offset)
	//fmt.Printf("BufferSize : %d\n", checkpoint.BufferSize)
	//fmt.Printf("ArchivedLSN: 0x%X\n", checkpoint.ArchivedLSN)
	//fmt.Printf("Checksum1  : 0x%X\n", checkpoint.Checksum1)
	//fmt.Printf("Checksum2  : 0x%X\n", checkpoint.Checksum2)
	//fmt.Printf("CurentFSP  : 0x%X\n", checkpoint.CuurentFSP)
	//fmt.Printf("Magic      : 0x%X\n", checkpoint.Magic)

	checkpoint2 := Checkpoint{}
	err = utils.ReadIntoStruct(file, &checkpoint2, cpsize)
	if err != nil {
		return err
	}
	//fmt.Println("================================================================================")
	//fmt.Println("Parsed second checkpoint data:")
	//fmt.Printf("Number     : 0x%X\n", checkpoint2.Number)
	//fmt.Printf("LSN        : 0x%X\n", checkpoint2.LSN)
	//fmt.Printf("Offset     : 0x%X\n", checkpoint2.Offset)
	//fmt.Printf("BufferSize : %d\n", checkpoint2.BufferSize)
	//fmt.Printf("ArchivedLSN: 0x%X\n", checkpoint2.ArchivedLSN)
	//fmt.Printf("Checksum1  : 0x%X\n", checkpoint2.Checksum1)
	//fmt.Printf("Checksum2  : 0x%X\n", checkpoint2.Checksum2)
	//fmt.Printf("CurentFSP  : 0x%X\n", checkpoint2.CuurentFSP)
	//fmt.Printf("Magic      : 0x%X\n", checkpoint2.Magic)
	//fmt.Println()
	//fmt.Println()

	return nil
}

func (parse *ParseRedo) MLOG_N_BYTES(data []byte, pos *uint64, MyType uint64) error {

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

func (parse *ParseRedo) MLOG_REC_SEC_DELETE_MARK(data []byte, pos *uint64) {

	value := utils.MatchReadFrom1(data[*pos:])
	*pos++

	offset := utils.MatchReadFrom2(data[*pos:])
	*pos += 2

	logs.Debug("values is ", value, " offset is ", offset)
}

func (parse *ParseRedo) GetPrimaryKey(Table ibdata.Tables) ([]*ibdata.Fields, error) {

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

func (parse *ParseRedo) GetColumnsByName(table ibdata.Tables, FieldName string) ibdata.Columns {
	for _, c := range table.Columns {
		if c.FieldName == FieldName {
			return c
		}
	}
	return ibdata.Columns{}
}

func (parse *ParseRedo) GetColumnByPos(Table ibdata.Tables, Pos uint64) (*ibdata.Columns, error) {
	for i, c := range Table.Columns {
		if uint64(i) == Pos {
			return &c, nil
		}
	}
	logs.Error("table ", Table.TableName, " have not found field pos ", Pos)
	return &ibdata.Columns{}, fmt.Errorf("field not found")
}

func (parse *ParseRedo) GetTableByTableID(TableID uint64) (ibdata.Tables, error) {
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

func (parse *ParseRedo) GetTableBySpaceID(SpaceID uint64) (ibdata.Tables, error) {
	for _, table := range parse.TableMap {
		if table.SpaceId == SpaceID {
			return table, nil
		}
	}
	return ibdata.Tables{}, fmt.Errorf("can't find table")
}

func (parse *ParseRedo) MakeSQL(table ibdata.Tables, PrimaryColumns []*ibdata.Fields, columns []*ibdata.Columns) {

	// TODO: deal with null value.

	// update statement
	var SetValues string
	for i, c := range columns {
		var SetValue string
		if c.FieldValue != nil {
			SetValue = fmt.Sprintf("`%s`='%v'", c.FieldName, c.FieldValue)
		} else {
			SetValue = fmt.Sprintf("`%s`=NULL", c.FieldName)
		}
		if i == (len(columns) - 1) {
			SetValues += SetValue
		} else {
			SetValues += SetValue + " and "
		}
	}

	var WhereConditions string

	for j, v := range PrimaryColumns {
		WhereCondition := fmt.Sprintf("`%s`='%v'", v.ColumnName, v.ColumnValue)
		if j == (len(PrimaryColumns) - 1) {
			WhereConditions += WhereCondition
		} else {
			WhereConditions += WhereCondition + " and "
		}
	}

	Query := fmt.Sprintf("update `%s`.`%s` set %s where %s;", table.DBName,
		table.TableName, SetValues, WhereConditions)

	logs.Debug("query is ", Query)
	fmt.Println(Query)
}

// Parse the undo record.
func (parse *ParseRedo) MLOG_UNDO_INSERT(data []byte, pos *uint64) error {

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
	*pos += 1

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
		*pos += 1

		TrxId := utils.MatchUllReadComPressed(data, pos)
		logs.Debug("TrxID is ", TrxId)

		RollPtr := utils.MatchUllReadComPressed(data, pos)
		logs.Debug("RollPtr is ", RollPtr)
	}

	Table, err := parse.GetTableByTableID(TableId)
	if err != nil {
		*pos = StartPos + DataLen
		return err
	}

	PrimaryFields, err := parse.GetPrimaryKey(Table)
	if err != nil {
		*pos = StartPos + DataLen
		return err
	}

	for _, v := range PrimaryFields {
		Column := parse.GetColumnsByName(Table, v.ColumnName)
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

			c, err := parse.GetColumnByPos(Table, Pos)
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
				parse.MakeSQL(Table, PrimaryFields, columns)
			} else if parse.TableName == "" {
				parse.MakeSQL(Table, PrimaryFields, columns)
			}
		} else if parse.DBName == "" && parse.TableName == "" {
			parse.MakeSQL(Table, PrimaryFields, columns)
		} else if parse.TableName != "" && parse.TableName == Table.TableName {
			parse.MakeSQL(Table, PrimaryFields, columns)
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

func (parse *ParseRedo) MLOG_UNDO_INIT(data []byte, pos *uint64) error {
	_, num, err := utils.MatchParseCompressed(data, *pos)
	if err != nil {
		return err
	}
	*pos += num

	return nil
}

func (parse *ParseRedo) MLOG_UNDO_HDR_REUSE(data []byte, pos *uint64) {
	utils.MatchUllReadComPressed(data, pos)
}

func (parse *ParseRedo) MLOG_UNDO_HDR_CREATE(data []byte, pos *uint64) error {
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

func (parse *ParseRedo) MLOG_WRITE_STRING(data []byte, pos *uint64) {

	// Parses a log record written by mlog_write_string
	offset := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	len := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	*pos += len
	logs.Debug("offset is ", offset, " len is ", len)

}

func (parse *ParseRedo) MLOG_FILE_OP(data []byte, pos *uint64, MyType uint64) {

	if MyType == MLOG_FILE_CREATE2 {
		flags := utils.MatchReadFrom4(data[*pos:])
		*pos += 4
		logs.Debug("flags is ", flags)
	}

	NameLen := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	*pos += NameLen

}

func (parse *ParseRedo) MLOG_REC_MARK(data []byte, pos uint64) {
	pos += 2
}

func (parse *ParseRedo) MLOG_REC_INSERT(data []byte, pos *uint64, mytype uint64) error {
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

func (parse *ParseRedo) MLOG_REC_CLUST_DELETE_MARK(data []byte, pos *uint64, mytype uint64) error {

	Pos, err := utils.ParseIndex(mytype == MLOG_COMP_REC_CLUST_DELETE_MARK, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = Pos
		// btr_cur_parse_del_mark_set_clust_rec
		// Parses the redo log record for delete marking or unmarking of a clustered
		// index record.
		flags := utils.MatchReadFrom1(data[*pos:])
		*pos += 1

		value := utils.MatchReadFrom1(data[*pos:])
		*pos += 1

		Pos, num, err := utils.MatchParseCompressed(data, *pos)
		if err != nil {
			return err
		}

		logs.Debug("num is ", num)
		*pos += num

		RollPtr := utils.MatchReadFrom7(data[*pos:])
		*pos += 7

		// TODO: should use MatchUllParseComPressed.
		TrxId := utils.MatchUllReadComPressed(data, pos)

		logs.Debug("flags is ", flags, "value is ", value, "Pos is ",
			Pos, "RollPtr is ", RollPtr, "TrxId is ", TrxId)

		offset := utils.MatchReadFrom2(data[*pos:])
		*pos += 2

		logs.Debug("page offset is ", offset)

	}
	return nil
}

func (parse *ParseRedo) MLOG_COMP_REC_SEC_DELETE_MARK(data []byte, pos *uint64) error {
	Pos, err := utils.ParseIndex(true, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = Pos
	}
	return nil
}

func (parse *ParseRedo) MLOG_REC_UPDATE_IN_PLACE(data []byte, pos *uint64, MyType uint64) error {

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
		*pos += 1

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

			*pos += 1
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

func (parse *ParseRedo) MLOG_REC_DELETE(data []byte, pos *uint64, MyType uint64) error {
	Pos, err := utils.ParseIndex(MyType == MLOG_COMP_REC_DELETE, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = Pos

		// page_cur_parse_delete_rec
		// Parses log record of a record delete on a page.
		offset := utils.MatchReadFrom2(data[*pos:])
		*pos += 2

		logs.Debug("offset is ", offset)

	}
	return nil
}

func (parse *ParseRedo) MLOG_LIST_DELETE(data []byte, pos *uint64, MyType uint64) error {
	Pos, err := utils.ParseIndex(MyType == MLOG_COMP_LIST_START_DELETE ||
		MyType == MLOG_COMP_LIST_END_DELETE, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = Pos

		// page_parse_delete_rec_list
		// Parses a log record of a record list end or start deletion.

		offset := utils.MatchReadFrom2(data[*pos:])
		*pos += 2

		logs.Debug("offset is ", offset)
	}
	return nil
}

func (parse *ParseRedo) MLOG_LIST_END_COPY_CREATED(data []byte, pos *uint64, MyType uint64) error {

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

func (parse *ParseRedo) MLOG_PAGE_REORGANIZE(data []byte, pos *uint64, MyType uint64) error {

	Pos, err := utils.ParseIndex(MyType != MLOG_PAGE_REORGANIZE, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = Pos

		if MyType == MLOG_ZIP_PAGE_REORGANIZE {
			level := utils.MatchReadFrom1(data[*pos:])
			*pos++
			logs.Debug("level is ", level)
		}
	}
	return nil
}

func (parse *ParseRedo) MLOG_ZIP_WRITE_NODE_PTR(data []byte, pos *uint64) {

	// TODO: confirm.

	offset := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	Zoffset := utils.MatchReadFrom2(data[*pos:])
	*pos += 2

	logs.Debug("offset is", offset, "Zoffset is ", Zoffset)

	// REC_NODE_PTR_SIZE
	*pos += 4
}

func (parse *ParseRedo) MLOG_ZIP_WRITE_BLOB_PTR(data []byte, pos uint64) {
	pos += 24
}

func (parse *ParseRedo) MLOG_ZIP_WRITE_HEADER(data []byte, pos *uint64) {
	// TODO: confirm.

	offset := uint64(data[*pos:][0])
	logs.Debug("offset is ", offset)

	*pos += 1
	DataLen := uint64(data[*pos:][0])
	*pos += 1

	*pos += DataLen
}

func (parse *ParseRedo) MLOG_ZIP_PAGE_COMPRESS(data []byte, pos *uint64) {
	size := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	TrailerSize := utils.MatchReadFrom2(data[*pos:])
	*pos += 2
	*pos += 8 + size + TrailerSize

	logs.Debug("size is:", size, "TrailerSize is :", TrailerSize)
}

func (parse *ParseRedo) MLOG_ZIP_PAGE_COMPRESS_NO_DATA(data []byte, pos *uint64) error {
	// TODO: confirm.

	Pos, err := utils.ParseIndex(true, data, *pos)
	if err != nil {
		return err
	} else {
		*pos = Pos
		level := utils.MatchReadFrom1(data[*pos:])
		logs.Debug("level is ", level)
		*pos += 1
	}
	return nil
}

func (parse *ParseRedo) ValidateLogHeader(LogType uint64, SpaceID uint64) bool {

	HaveType := true

	switch LogType {

	case MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES, MLOG_8BYTES:

	case MLOG_REC_SEC_DELETE_MARK:

	case MLOG_UNDO_INSERT:

	case MLOG_UNDO_ERASE_END:

	case MLOG_UNDO_INIT:
	case MLOG_UNDO_HDR_DISCARD:

	case MLOG_UNDO_HDR_REUSE:

	case MLOG_UNDO_HDR_CREATE:

	case MLOG_IBUF_BITMAP_INIT:

	case MLOG_INIT_FILE_PAGE, MLOG_INIT_FILE_PAGE2:

	case MLOG_WRITE_STRING:

	case MLOG_MULTI_REC_END:

	case MLOG_DUMMY_RECORD:

	case MLOG_FILE_RENAME, MLOG_FILE_CREATE,
		MLOG_FILE_DELETE, MLOG_FILE_CREATE2,
		MLOG_FILE_RENAME2, MLOG_FILE_NAME:

	case MLOG_REC_MIN_MARK, MLOG_COMP_REC_MIN_MARK:

	case MLOG_PAGE_CREATE, MLOG_COMP_PAGE_CREATE:

	case MLOG_REC_INSERT, MLOG_COMP_REC_INSERT:

	case MLOG_REC_CLUST_DELETE_MARK, MLOG_COMP_REC_CLUST_DELETE_MARK:
	case MLOG_COMP_REC_SEC_DELETE_MARK:

	case MLOG_REC_UPDATE_IN_PLACE, MLOG_COMP_REC_UPDATE_IN_PLACE:

	case MLOG_REC_DELETE, MLOG_COMP_REC_DELETE:

	case MLOG_LIST_END_DELETE,
		MLOG_COMP_LIST_END_DELETE,
		MLOG_LIST_START_DELETE,
		MLOG_COMP_LIST_START_DELETE:

	case MLOG_LIST_END_COPY_CREATED, MLOG_COMP_LIST_END_COPY_CREATED:

	case MLOG_PAGE_REORGANIZE, MLOG_COMP_PAGE_REORGANIZE, MLOG_ZIP_PAGE_REORGANIZE:

	case MLOG_ZIP_WRITE_NODE_PTR:
	case MLOG_ZIP_WRITE_BLOB_PTR:

	case MLOG_ZIP_WRITE_HEADER:

	case MLOG_ZIP_PAGE_COMPRESS:

	case MLOG_ZIP_PAGE_COMPRESS_NO_DATA:
	case MLOG_CHECKPOINT:
	case MLOG_COMP_PAGE_CREATE_RTREE, MLOG_PAGE_CREATE_RTREE:
	case MLOG_TRUNCATE:
	case MLOG_INDEX_LOAD:
	default:
		HaveType = false
		break
	}

	//_, err := parse.GetTableBySpaceID(SpaceID)
	//if err != nil || !HaveType {
	//	return false
	//}

	return HaveType
}
