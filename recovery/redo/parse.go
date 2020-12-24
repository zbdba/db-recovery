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
	"github.com/zbdba/db-recovery/recovery/ibdata"
	"github.com/zbdba/db-recovery/recovery/logs"
	"github.com/zbdba/db-recovery/recovery/utils"
	"io"
	"os"
)

// Parse for redo log
type Parse struct {
	// Store table info.
	TableMap map[uint64]ibdata.Tables

	// The table name which you want to recovery.
	TableName string

	// The database name which you want to recovery.
	DBName string
}

// NewParse ...
func NewParse(tableName string, dbName string) *Parse {
	return &Parse{
		TableMap:  make(map[uint64]ibdata.Tables),
		TableName: tableName,
		DBName:    dbName,
	}
}

// ParseDictPage ...
func (parse *Parse) ParseDictPage(ibdataPath string) error {
	// get data dict
	ibdataParse := ibdata.NewParse()
	err := ibdataParse.ParseDictPage(ibdataPath)
	parse.TableMap = ibdataParse.TableMap
	return err
}

// ParseRedoLogs parse the redo log file
// The redo log file have four parts:
// 1. redo log header
// 2. checkpoint 1
// 3. checkpoint 2
// 4. redo block
// And every parts is 512 bytes, there will be many
// redo blocks which store the redo record.
func (parse *Parse) ParseRedoLogs(logFileList []string) error {
	var data []byte
	for _, LogFile := range logFileList {
		file, err := os.Open(LogFile)
		if err != nil {
			logs.Error("Error while opening file, the error is ", err)
			return err
		}

		// Parse read redo log header
		if err = parse.readHeader(file); err != nil {
			return err
		}

		// Parse read redo log checkpoint
		if err = parse.readCheckpoint(file); err != nil {
			return err
		}

		// Move to the start of the logs
		// Current position is 512 + 512 + 512 = 1536 and logs start at 2048
		if pos, err := file.Seek(BlockSize, io.SeekCurrent); err == nil {
			logs.Debug("Current position: ", pos)
		} else {
			return err
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
			dataLen, firstRecord, readErr := parse.readRedoBlockHeader(&pos, d)
			if readErr != nil || dataLen == 0 {
				break
			}

			// LOG_BLOCK_TRL_SIZE, for checksum.
			if dataLen >= BlockSize {
				// TODO: const
				dataLen -= 4
			}

			// Sometimes the first block may not be the beginning of the log record,
			// it may be the end of the previous log record. we will make an error
			// when we parse directly. At this time, we will start parsing directly
			// from the position specified by first record. If first record
			// is equal to 0, this block is used by all previous log records, we just skip it.
			if len(data) == 0 && firstRecord == uint64(0) {
				continue
			}

			if len(data) == 0 && firstRecord > 12 {
				pos += firstRecord - 12
			}

			// Add all redo block data.
			data = append(data, d[pos:dataLen]...)
		}
		file.Close()
	}

	// Start parse redo log data.
	err := parse.parseRedoBlockData(data)
	if err != nil {
		logs.Error("parse redo block data failed, error: ", err.Error())
	}
	return err
}

// Parse the redo block data, it consists of many redo records.
// There are about 55 redo log type, and every redo log type have different data.
// We just want get the MLOG_UNDO_INSERT type redo record which store the undo info.
// But due to the design of redo, we must parse out each type in order.
func (parse *Parse) parseRedoBlockData(data []byte) error {
	var pos uint64
	for {
		if (int64(len(data)) - int64(pos)) < 5 {
			break
		}
		if (uint64(len(data)) - pos) < 5 {
			break
		}

		// Read initial log record.
		logType := utils.MatchReadFrom1(data[pos:])
		pos++
		startPos := pos

		if logType == MLOG_MULTI_REC_END {
			// don't parse MLOG_MULTI_REC_END type redo log.
			continue
		}

		// Get redo log type.
		logType = uint64(int(logType) & ^MLOG_SINGLE_REC_FLAG)

		if logType != MLOG_CHECKPOINT {

			// Get space id
			spaceID, num, err := utils.MatchParseCompressed(data, pos)
			pos += num
			if err != nil {
				logs.Error("get log record space id failed, the error is ", err)
				return err

			}
			// Get the page no
			pageNo, num, err := utils.MatchParseCompressed(data, pos)
			pos += num
			if err != nil {
				logs.Error("get log record page number failed, the error is ", err)
				return err
			}

			if !parse.validateLogHeader(logType, spaceID) {
				pos = startPos
				continue
			}

			logs.Debug("logType is:", logType, "spaceID is:", spaceID, "pageNo is:", pageNo)

			table, err := parse.getTableBySpaceID(spaceID)
			if err != nil {
				logs.Error("can't find table, space id is ", spaceID)
			} else {
				logs.Debug("table name is ", table.TableName)
			}
		} else {
			logs.Debug("logType is:", logType)
		}

		switch logType {

		case MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES, MLOG_8BYTES:
			logs.Debug("start parse MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES, MLOG_8BYTES log record")
			err := parse.MLOG_N_BYTES(data, &pos, logType)
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
			parse.MLOG_FILE_OP(data, &pos, logType)

		case MLOG_REC_MIN_MARK, MLOG_COMP_REC_MIN_MARK:
			logs.Debug("start parse MLOG_REC_MIN_MARK, MLOG_COMP_REC_MIN_MARK log record")
			pos += 2

		case MLOG_PAGE_CREATE, MLOG_COMP_PAGE_CREATE:
			logs.Debug("start parse MLOG_PAGE_CREATE, MLOG_COMP_PAGE_CREATE log record")
			continue

		case MLOG_REC_INSERT, MLOG_COMP_REC_INSERT:
			logs.Debug("start parse MLOG_REC_INSERT, MLOG_COMP_REC_INSERT log record")
			err := parse.MLOG_REC_INSERT(data, &pos, logType)
			if err != nil {
				return err
			}

		case MLOG_REC_CLUST_DELETE_MARK, MLOG_COMP_REC_CLUST_DELETE_MARK:
			logs.Debug("start parse MLOG_REC_CLUST_DELETE_MARK, MLOG_COMP_REC_CLUST_DELETE_MARK log record")
			err := parse.MLOG_REC_CLUST_DELETE_MARK(data, &pos, logType)
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
			err := parse.MLOG_REC_UPDATE_IN_PLACE(data, &pos, logType)
			if err != nil {
				break
			}

		case MLOG_REC_DELETE, MLOG_COMP_REC_DELETE:
			logs.Debug("start parse MLOG_REC_DELETE, MLOG_COMP_REC_DELETE log record")
			err := parse.MLOG_REC_DELETE(data, &pos, logType)
			if err != nil {
				break
			}

		case MLOG_LIST_END_DELETE,
			MLOG_COMP_LIST_END_DELETE,
			MLOG_LIST_START_DELETE,
			MLOG_COMP_LIST_START_DELETE:
			logs.Debug("start parse MLOG_LIST_END_DELETE,MLOG_COMP_LIST_END_DELETE," +
				"MLOG_LIST_START_DELETE,MLOG_COMP_LIST_START_DELETE log record")
			err := parse.MLOG_LIST_DELETE(data, &pos, logType)
			if err != nil {
				break
			}

		case MLOG_LIST_END_COPY_CREATED, MLOG_COMP_LIST_END_COPY_CREATED:
			logs.Debug("start parse MLOG_LIST_END_COPY_CREATED, MLOG_COMP_LIST_END_COPY_CREATED log record")
			err := parse.MLOG_LIST_END_COPY_CREATED(data, &pos, logType)
			if err != nil {
				break
			}

		case MLOG_PAGE_REORGANIZE, MLOG_COMP_PAGE_REORGANIZE, MLOG_ZIP_PAGE_REORGANIZE:
			logs.Debug("start parse MLOG_PAGE_REORGANIZE, MLOG_COMP_PAGE_REORGANIZE, " +
				"MLOG_ZIP_PAGE_REORGANIZE log record")
			err := parse.MLOG_PAGE_REORGANIZE(data, &pos, logType)
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
			logs.Debug("unknown rMLOG_REC_UPDATE_IN_PLACEedo type, break.")
			break
		}
	}
	return nil
}

// Parse the redo block header.
func (parse *Parse) readRedoBlockHeader(pos *uint64, d []byte) (uint64, uint64, error) {

	logBlockNo := utils.MatchReadFrom4(d)
	*pos += 4

	dataLen := utils.MatchReadFrom2(d[*pos:])
	*pos += 2

	firstRecord := utils.MatchReadFrom2(d[*pos:])
	*pos += 2

	checkpointNo := utils.MatchReadFrom4(d[*pos:])
	*pos += 4

	logs.Debug("==================================")
	logs.Debug("logBlockNo:", logBlockNo)
	logs.Debug("dataLen:", dataLen)
	logs.Debug("firstRecord:", firstRecord)
	logs.Debug("checkpointNo:", checkpointNo)
	logs.Debug("==================================")
	logs.Debug("")

	return dataLen, firstRecord, nil
}

func (parse *Parse) readHeader(file *os.File) error {

	pos := 0
	data, err := utils.ReadNextBytes(file, 512)
	if err != nil {
		return err
	}

	logHeaderFormat := utils.MatchReadFrom4(data[pos:])
	logs.Debug("LOG_HEADER_FORMAT is ", logHeaderFormat)
	pos += 8

	logHeaderStartLsn := utils.MatchReadFrom8(data[pos:])
	logs.Debug("LOG_HEADER_START_LSN is ", logHeaderStartLsn)

	return nil
}

func (parse *Parse) readCheckpoint(file *os.File) error {

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
	//fmt.Printf("CurrentFSP  : 0x%X\n", checkpoint.CurrentFSP)
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
	//fmt.Printf("CurrentFSP  : 0x%X\n", checkpoint2.CurrentFSP)
	//fmt.Printf("Magic      : 0x%X\n", checkpoint2.Magic)
	//fmt.Println()
	//fmt.Println()

	return nil
}

func (parse *Parse) getTableBySpaceID(spaceID uint64) (ibdata.Tables, error) {
	for _, table := range parse.TableMap {
		if table.SpaceId == spaceID {
			return table, nil
		}
	}
	return ibdata.Tables{}, fmt.Errorf("can't find table")
}

func (parse *Parse) validateLogHeader(LogType uint64, SpaceID uint64) bool {

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

	//_, err := parse.getTableBySpaceID(SpaceID)
	//if err != nil || !HaveType {
	//	return false
	//}

	return HaveType
}

func (parse *Parse) makeSQL(table ibdata.Tables, primaryColumns []*ibdata.Fields, columns []*ibdata.Columns) {

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

	for j, v := range primaryColumns {
		WhereCondition := fmt.Sprintf("`%s`='%v'", v.ColumnName, v.ColumnValue)
		if j == (len(primaryColumns) - 1) {
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
