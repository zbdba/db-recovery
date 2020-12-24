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

package ibdata

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/zbdba/db-recovery/recovery/logs"
	"github.com/zbdba/db-recovery/recovery/utils"
)

// Parse for ibdata
type Parse struct {
	// store table's struct.
	TableMap map[uint64]Tables
	// TODO: change to map. store page data.
	D *sync.Map
}

// NewParse create parse
func NewParse() *Parse {
	return &Parse{
		TableMap: make(map[uint64]Tables),
		D:        new(sync.Map),
	}
}

// ParseDictPage get table struct in dict page.
func (parse *Parse) ParseDictPage(path string) error {

	pages, err := parse.parseFile(path)
	if err != nil {
		logs.Error("parse system data file failed, error: ", err.Error())
		return err
	}

	var systemPage Page
	for _, page := range pages {
		if page.fh.FIL_PAGE_OFFSET == SystemPageIdx {
			systemPage, err = parse.parseSysPageHeader(page)
			if err != nil {
				return err
			}
			break
		}
	}

	// Get all dict pages, and use map to store all dict page's data.
	for _, page := range pages {

		// TODO: Parse undo page from sys data file.
		//if parse.fh.FIL_PAGE_TYPE == uint64(2) {
		//	P.ParseUndoPageHeader(&parse)
		//}

		if page.fh.FIL_PAGE_OFFSET == systemPage.sp.DICT_HDR_TABLES ||
			page.fh.FIL_PAGE_OFFSET == systemPage.sp.DICT_HDR_COLUMNS ||
			page.fh.FIL_PAGE_OFFSET == systemPage.sp.DICT_HDR_INDEXES ||
			page.fh.FIL_PAGE_OFFSET == systemPage.sp.DICT_HDR_FIELDS {

			var ds []DataDict
			parse.parsePageHeader(&page)
			ds = append(ds,
				DataDict{
					IndexId:    page.ph.PAGE_INDEX_ID,
					PageOffset: page.fh.FIL_PAGE_OFFSET,
					data:       page.OriginalData,
					pos:        len(page.OriginalData) - len(page.data)})
			parse.D.Store(page.ph.PAGE_INDEX_ID, ds)

			logs.Debug("page offset is ", page.fh.FIL_PAGE_OFFSET,
				"indexId is", page.ph.PAGE_INDEX_ID, " data len is ", len(ds))

		} else {
			f := func(k, v interface{}) bool {
				if k == page.ph.PAGE_INDEX_ID {
					ds := v.([]DataDict)
					parse.parsePageHeader(&page)
					ds = append(ds,
						DataDict{
							IndexId:    page.ph.PAGE_INDEX_ID,
							PageOffset: page.fh.FIL_PAGE_OFFSET,
							data:       page.OriginalData,
							pos:        len(page.OriginalData) - len(page.data)})
					parse.D.Store(page.ph.PAGE_INDEX_ID, ds)

					logs.Debug("page offset is ", page.fh.FIL_PAGE_OFFSET,
						"indexId is", page.ph.PAGE_INDEX_ID, " data len is ", len(ds))
				}
				return true
			}
			parse.D.Range(f)
		}
	}

	if err := parse.getAllTables(); err != nil {
		return err
	}

	if err := parse.getAllColumns(); err != nil {
		return err
	}

	if err := parse.getAllIndexes(); err != nil {
		return err
	}

	if err := parse.getAllFields(); err != nil {
		return err
	}

	// Sort fields
	parse.sortColumns()

	// Move primary key first.
	parse.movePrimaryKeyFirst()

	// Add internal columns.
	parse.addInternalColumns()

	return nil
}

// ParseDataPage read table data from data file, should identified the file
// path/database name/table name which can confirm the table info.
// You should identify the IsRecovery to confirm
// whether recovery table data or just read table data.
func (parse *Parse) ParseDataPage(path string, dbName string, tableName string, isRecovery bool) error {
	pages, err := parse.parseFile(path)
	if err != nil {
		return err
	}

	// Get table fields info from data dict.
	fields, err := parse.getTableColumnsFromDict(dbName, tableName)
	if err != nil {
		logs.Error("get fields from dict failed, error: ", err.Error(), " DBName: ", dbName, " TableName: ", tableName)
		return err
	}

	for _, page := range pages {

		logs.Debug("page.fh.FIL_PAGE_TYPE is ", page.fh.FIL_PAGE_TYPE,
			"page.fh.FIL_PAGE_OFFSET", page.fh.FIL_PAGE_OFFSET)

		parse.parsePageHeader(&page)

		if isRecovery {
			// If page have delete data, the page free and garbage should not be zero.
			if page.ph.PAGE_N_RECS == uint64(0) && page.ph.PAGE_FREE == uint64(0) {
				continue
			}
			if page.ph.PAGE_FREE > uint64(0) && page.ph.PAGE_GARBAGE == uint64(0) {
				continue
			}
			if page.ph.PAGE_FREE == uint64(0) && page.ph.PAGE_GARBAGE == uint64(0) {
				continue
			}
			if page.ph.PAGE_FREE > uint64(16384) {
				continue
			}
		}

		// Should be index page and should be leaf node.
		// Only recovery cluster index, ignore secondary index.
		// TODO: only secondary index's PAGE_MAX_TRX_ID > 0 ?
		if page.fh.FIL_PAGE_TYPE == FilPageIndex &&
			page.ph.PAGE_LEVEL == uint64(0) &&
			page.ph.PAGE_MAX_TRX_ID == uint64(0) &&
			!(isRecovery && page.ph.PAGE_FREE == 0) {
			allColumns := parse.parsePage(page.OriginalData, len(page.OriginalData)-len(page.data),
				fields, isRecovery, page.ph.PAGE_FREE)
			if len(allColumns) == 0 {
				logs.Info("have no data")
				continue
			}

			// Make replace into statement.
			parse.makeReplaceIntoStatement(allColumns, tableName, dbName)
		}
	}
	return nil
}

// parseFile parse innodb data file
// The data file is composed of multiple pages, each page is 16kb by default.
func (parse *Parse) parseFile(path string) ([]Page, error) {
	var pages []Page

	file, err := os.Open(path)
	if err != nil {
		logs.Error("Error while opening file, error: ", err.Error())
		return pages, err
	}
	defer file.Close()

	pageMap := make(map[uint64]string)

	for {
		// Read a page from data file.
		data, err := utils.ReadNextBytes(file, DefaultPageSize)
		if len(data) < DefaultPageSize {
			if len(data) != 0 {
				logs.Warn("next data bytes no more than 1 page, please check DefaultPageSize!")
			}
			break
		}
		if err != nil {
			logs.Error("read data from file failed, error: ", err.Error())
			return pages, err
		}

		// Parse the page file header.
		page, err := parse.parseFilHeader(data)
		if err != nil {
			logs.Error("parseFilHeader, error: ", err.Error())
			return pages, err
		}

		// Store the page.
		page.OriginalData = data

		// TODO: why have the same pages?
		// Store the page into the pageMap.
		if _, ok := pageMap[page.fh.FIL_PAGE_OFFSET]; !ok {
			pages = append(pages, page)
			pageMap[page.fh.FIL_PAGE_OFFSET] = "have"
		}
	}
	return pages, err
}

func (parse *Parse) parseFilHeader(data []byte) (Page, error) {

	var page Page
	pos := 0

	// Get index id
	IndexId := utils.MatchReadFrom8(data[(pos + 38 + 28):])
	logs.Debug("Index is is ", IndexId)
	page.ph.PAGE_INDEX_ID = IndexId

	// Parse Fil Header
	SpaceId := utils.MatchReadFrom4(data[pos:])
	logs.Debug("SpaceId:", SpaceId)
	page.fh.FIL_PAGE_SPACE = SpaceId
	pos += 4

	PageOffset := utils.MatchReadFrom4(data[pos:])
	logs.Debug("PageOffset:", PageOffset)
	page.fh.FIL_PAGE_OFFSET = PageOffset
	pos += 4

	PagePrev := utils.MatchReadFrom4(data[pos:])
	logs.Debug("PagePrev:", PagePrev)
	page.fh.FIL_PAGE_PREV = PagePrev
	pos += 4

	PageNext := utils.MatchReadFrom4(data[pos:])
	logs.Debug("PagePrev:", PageNext)
	page.fh.FIL_PAGE_NEXT = PageNext
	pos += 4

	PageLsn := utils.MatchReadFrom8(data[pos:])
	logs.Debug("PageLsn:", PageLsn)
	page.fh.FIL_PAGE_LSN = PageLsn
	pos += 8

	PageType := utils.MatchReadFrom2(data[pos:])
	logs.Debug("PageType:", PageType)
	page.fh.FIL_PAGE_TYPE = PageType
	pos += 2

	PageFileFlushLsn := utils.MatchReadFrom8(data[pos:])
	logs.Debug("PageFileFlushLsn:", PageFileFlushLsn)
	page.fh.FIL_PAGE_LSN = PageFileFlushLsn
	pos += 8

	PageArchLogNo := utils.MatchReadFrom4(data[pos:])
	logs.Debug("PageArchLogNo:", PageArchLogNo)
	page.fh.FIL_PAGE_ARCH_LOG_NO = PageArchLogNo
	pos += 4

	page.data = data[pos:]

	return page, nil
}

// sortColumns sort columns by field position.
// // TODO: why some table fields is not sort?
func (parse *Parse) sortColumns() {
	var sortedColumns []Columns
	for _, table := range parse.TableMap {
		for i, field := range table.Columns {
			if field.FieldPos == uint64(i) {
				sortedColumns = append(sortedColumns, field)
			}
		}
		table.Columns = sortedColumns
	}
}

// move the primary key field to the first.
func (parse *Parse) movePrimaryKeyFirst() {

	for tableID, table := range parse.TableMap {
		for _, idx := range table.Indexes {
			if idx.Name == "PRIMARY" {
				table.Columns = parse.moveColumns2First(idx.Fields, table.Columns)
			}
		}
		parse.TableMap[tableID] = table
	}
}

// moveColumns2First ...
func (parse *Parse) moveColumns2First(fields []*Fields, columns []Columns) []Columns {
	for i := 0; i < len(columns); i++ {
		for j, field := range fields {
			if field.ColumnName == columns[i].FieldName {
				tempField := columns[i]
				columns = append(columns[:j], columns[j+1:]...)
				columns = append([]Columns{tempField}, columns...)
			}
		}
	}
	return columns
}

// Table have some internal filed in MySQL, such as DB_TRX_ID/DB_ROLL_PTR/DB_ROW_ID.
// When we parse a table row, we should add this internal columns.
// If table have primary key, there should have two internal fields: DB_TRX_ID, DB_ROLL_PTR.
// otherwise there should have three internal fields: DB_ROW_ID, DB_TRX_ID, DB_ROLL_PTR.
func (parse *Parse) addInternalColumns() {
	for tableID, table := range parse.TableMap {
		for _, idx := range table.Indexes {
			if idx.Name == "PRIMARY" {
				// primary key exists, add DB_TRX_ID, DB_ROLL_PTR after primary field.
				TrxColumns := parse.addInternalColumn("DB_TRX_ID", tableID, idx.FieldNum)
				RollPtrColumns := parse.addInternalColumn("DB_ROLL_PTR", tableID, idx.FieldNum+1)

				table.Columns = addColumns(table.Columns, int(idx.FieldNum), TrxColumns)
				table.Columns = addColumns(table.Columns, int(idx.FieldNum+1), RollPtrColumns)
				parse.TableMap[tableID] = table
			} else if idx.Name == "GEN_CLUST_INDEX" {
				// primary key doesn't exist, should add DB_ROW_ID field.
				// TODO: remove repeat code.
				rowIDColumns := parse.addInternalColumn("DB_ROW_ID", tableID, 0)
				trxColumns := parse.addInternalColumn("DB_TRX_ID", tableID, 1)
				rollPtrColumns := parse.addInternalColumn("DB_ROLL_PTR", tableID, 2)

				table.Columns = addColumns(table.Columns, 0, rowIDColumns)
				table.Columns = addColumns(table.Columns, 1, trxColumns)
				table.Columns = addColumns(table.Columns, 2, rollPtrColumns)
				parse.TableMap[tableID] = table
			}
		}
	}
}

// addColumns ...
func addColumns(columns []Columns, index int, column Columns) []Columns {
	rear := append([]Columns{}, columns[index:]...)
	return append(append(columns[:index], column), rear...)
}

func (parse *Parse) addInternalColumn(fieldName string, tableID uint64, fieldPos uint64) Columns {
	var columns Columns
	switch fieldName {
	case "DB_ROW_ID":
		columns = Columns{FieldName: "DB_ROW_ID", FieldType: utils.DATA_MISSING, FieldPos: 0,
			FieldLen: 6, IsNUll: false, TableID: tableID}
	case "DB_TRX_ID":
		columns = Columns{FieldName: "DB_TRX_ID", FieldType: utils.DATA_MISSING,
			FieldPos: fieldPos, FieldLen: 6, IsNUll: false, TableID: tableID}
	case "DB_ROLL_PTR":
		columns = Columns{FieldName: "DB_ROLL_PTR", FieldType: utils.DATA_MISSING,
			FieldPos: fieldPos, FieldLen: 7, IsNUll: false, TableID: tableID}
	}
	return columns
}

// Get all column info from sys column dict tables.
func (parse *Parse) getAllColumns() error {
	logs.Debug("start parse sys_columns.")

	var AllPageColumns [][]Columns
	columns := parse.makeSysColumnsColumns()
	v, ok := parse.D.Load(SysColumnsIdx)
	if !ok {
		ErrMsg := fmt.Sprintf("sys column's page have not found")
		logs.Error(ErrMsg)
		return fmt.Errorf(ErrMsg)

	}

	dts := v.([]DataDict)
	for _, dt := range dts {

		// Start parse sys column table page.
		AllColumns := parse.parsePage(dt.data, dt.pos, columns, false, 0)
		AllPageColumns = append(AllPageColumns, AllColumns...)
	}

	for _, c := range AllPageColumns {

		v, ok := parse.TableMap[c[0].FieldValue.(uint64)]
		if ok {
			t := v

			IsNull := false
			IsUnsigned := false
			var MySQLType uint64

			// #define DATA_NOT_NULL	256
			// this is ORed to the precise type when the column is declared as NOT NULL
			// TODO: const
			if c[6].FieldValue.(uint64)&256 == 0 {
				IsNull = true
				t.NullCount++
			}

			// #define DATA_UNSIGNED	512
			usign := c[6].FieldValue.(uint64) & 512
			if usign != 0 {
				IsUnsigned = true
			}

			// reference MySQL dtype_get_mysql_type method.
			// mysql-5.7.19/storage/innobase/include/data0type.ic
			MySQLType = c[6].FieldValue.(uint64) & 0xFF

			IsBinary := false
			if c[5].FieldValue.(uint64) == utils.DATA_BLOB {
				if (c[6].FieldValue.(uint64) & 1024) != 0 {
					// this is text type.
					IsBinary = true
				}
			}

			var TempFieldLen uint64 = 0
			if c[5].FieldValue.(uint64) != utils.DATA_BINARY {
				TempFieldLen = c[7].FieldValue.(uint64)
			}

			t.Columns = append(t.Columns,
				Columns{
					FieldName: c[4].FieldValue.(string),
					FieldType: c[5].FieldValue.(uint64),
					MySQLType: MySQLType,
					FieldPos:  c[1].FieldValue.(uint64),
					FieldLen:  TempFieldLen,
					IsNUll:    IsNull, IsUnsigned: IsUnsigned,
					TableID:  c[0].FieldValue.(uint64),
					IsBinary: IsBinary})

			parse.TableMap[c[0].FieldValue.(uint64)] = t
		}
	}
	return nil
}

// Get all index info from sys index dict table.
func (parse *Parse) getAllIndexes() error {
	logs.Debug("start parse sys_index.")

	var allPageColumns [][]Columns
	var err error

	columns := parse.makeSysIndexesColumns()
	v, ok := parse.D.Load(SysIndexesIdx)
	if !ok {
		err = errors.New("sys indexes' page have not found")
		logs.Error(err.Error())
		return err
	}

	dts := v.([]DataDict)
	for _, dt := range dts {
		// Start parse sys indexes table pages.
		allColumns := parse.parsePage(dt.data, dt.pos, columns, false, 0)
		allPageColumns = append(allPageColumns, allColumns...)

	}

	for _, columns := range allPageColumns {
		var index map[uint64]Indexes
		v, ok := parse.TableMap[columns[0].FieldValue.(uint64)]
		if ok {
			t := v

			if t.Indexes == nil {
				// Use map to store index, and the key is the index id. value is index info.
				index = make(map[uint64]Indexes)
			} else {
				index = t.Indexes
			}

			index[columns[1].FieldValue.(uint64)] =
				Indexes{
					Id:        columns[1].FieldValue.(uint64),
					Name:      columns[4].FieldValue.(string),
					IndexType: columns[6].FieldValue.(uint64),
					FieldNum:  columns[5].FieldValue.(uint64)}

			t.Indexes = index
			parse.TableMap[columns[0].FieldValue.(uint64)] = t
		} else {
			err = errors.New("Table ID have not found " + fmt.Sprint(columns[0].FieldValue))
			logs.Error(err.Error())
		}
	}
	return err
}

// Get all fields from sys fields dict table.
func (parse *Parse) getAllFields() error {
	logs.Debug("start parse sys_fields.")

	var allPageColumns [][]Columns
	columns := parse.makeSysFieldsColumns()
	v, ok := parse.D.Load(SysFieldsIdx)
	if !ok {
		ErrMsg := fmt.Sprintf("sys field's page have not found")
		logs.Error(ErrMsg)
		return fmt.Errorf(ErrMsg)
	}

	dts := v.([]DataDict)
	for _, dt := range dts {
		// Start parse sys fields table pages.
		alColumns := parse.parsePage(dt.data, dt.pos, columns, false, 0)
		allPageColumns = append(allPageColumns, alColumns...)
	}

	// store all fields into index map
	indexFieldsMap := make(map[uint64][]*Fields)
	for _, columns := range allPageColumns {
		v, ok := indexFieldsMap[columns[0].FieldValue.(uint64)]
		if ok {
			v = append(v, &Fields{
				ColumnPos:  columns[1].FieldValue.(uint64),
				ColumnName: columns[4].FieldValue.(string),
			})
			indexFieldsMap[columns[0].FieldValue.(uint64)] = v
		} else {
			// TODO: nil value
			var fields []*Fields
			// TODO: Column Type
			fields = append(fields, &Fields{
				ColumnPos:  columns[1].FieldValue.(uint64),
				ColumnName: columns[4].FieldValue.(string),
			})
			indexFieldsMap[columns[0].FieldValue.(uint64)] = fields
		}
	}

	// Scan all table index and get fields from index map
	for tableID, table := range parse.TableMap {
		// Scan table index's array.
		for IndexId, idx := range table.Indexes {
			v, ok := indexFieldsMap[IndexId]
			if ok {
				// TODO: confirm, should store map again?
				idx.Fields = v
			}
			table.Indexes[IndexId] = idx
		}
		parse.TableMap[tableID] = table
	}

	return nil
}

// Get all table info from sys dict tables.
func (parse *Parse) getAllTables() error {
	logs.Debug("start parse sys_tables.")

	columns := parse.makeSysTablesColumns()
	v, ok := parse.D.Load(SysTablesIdx)
	if !ok {
		err := errors.New("sys table's page have not found")
		logs.Error(err.Error())
		return err
	}

	dts := v.([]DataDict)
	for _, dt := range dts {

		// parse sys table page.
		allColumns := parse.parsePage(dt.data, dt.pos, columns, false, 0)

		for _, columns := range allColumns {
			// database and table name, for example: zbdba3/jingbo_test
			var tableName string
			var databaseName string
			if strings.Contains(columns[0].FieldValue.(string), "/") {
				n := strings.Split(columns[0].FieldValue.(string), "/")
				databaseName = n[0]
				tableName = n[1]
			} else {
				tableName = columns[0].FieldValue.(string)
			}
			parse.TableMap[columns[3].FieldValue.(uint64)] =
				Tables{
					DBName:    databaseName,
					TableName: tableName,
					NullCount: 0,
					SpaceId:   columns[9].FieldValue.(uint64)}
		}
	}
	return nil
}

func (parse *Parse) parseSysPageHeader(page Page) (Page, error) {

	pos := 0
	d := page.data

	dictHdrRowId := utils.MatchReadFrom8(d[pos:])
	pos += 8
	logs.Debug("dictHdrRowId:", dictHdrRowId)
	page.sp.DICT_HDR_ROW_ID = dictHdrRowId

	dictHdrTableId := utils.MatchReadFrom8(d[pos:])
	pos += 8
	logs.Debug("dictHdrTableId:", dictHdrTableId)
	page.sp.DICT_HDR_TABLE_ID = dictHdrTableId

	dictHdrIndexId := utils.MatchReadFrom8(d[pos:])
	pos += 8
	logs.Debug("dictHdrIndexId:", dictHdrIndexId)
	page.sp.DICT_HDR_INDEX_ID = dictHdrIndexId

	dictHdrMaxSpaceId := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("dictHdrMaxSpaceId:", dictHdrMaxSpaceId)
	page.sp.DICT_HDR_MAX_SPACE_ID = dictHdrMaxSpaceId

	dictHdrMaxIdLow := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("dictHdrMaxIdLow:", dictHdrMaxIdLow)
	page.sp.DICT_HDR_MIX_ID_LOW = dictHdrMaxIdLow

	dictHdrTables := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("dictHdrTables:", dictHdrTables)
	page.sp.DICT_HDR_TABLES = dictHdrTables

	dictHdrTableIds := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("dictHdrTableIds:", dictHdrTableIds)
	page.sp.DICT_HDR_TABLE_IDS = dictHdrTableIds

	dictHdrColumns := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("dictHdrColumns:", dictHdrColumns)
	page.sp.DICT_HDR_COLUMNS = dictHdrColumns

	dictHdrIndexes := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("dictHdrIndexes:", dictHdrIndexes)
	page.sp.DICT_HDR_INDEXES = dictHdrIndexes

	dictHdrFields := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("dictHdrFields:", dictHdrFields)
	page.sp.DICT_HDR_FIELDS = dictHdrFields

	return page, nil
}

func (parse *Parse) parsePageHeader(page *Page) *Page {

	d := page.data
	pos := 0

	pageNDirSlots := utils.MatchReadFrom2(d[pos:])
	logs.Debug("pageNDirSlots:", pageNDirSlots)
	page.ph.PAGE_N_DIR_SLOTS = pageNDirSlots
	pos += 2

	pageHeapTop := utils.MatchReadFrom2(d[pos:])
	logs.Debug("pageHeapTop:", pageHeapTop)
	page.ph.PAGE_HEAP_TOP = pageHeapTop
	pos += 2

	pageNHeap := utils.MatchReadFrom2(d[pos:])
	logs.Debug("pageNHeap:", pageNHeap)
	page.ph.PAGE_N_HEAP = pageNHeap
	pos += 2

	pageFree := utils.MatchReadFrom2(d[pos:])
	logs.Debug("pageFree:", pageFree)
	page.ph.PAGE_FREE = pageFree
	pos += 2

	pageGarBage := utils.MatchReadFrom2(d[pos:])
	logs.Debug("pageGarBage:", pageGarBage)
	page.ph.PAGE_GARBAGE = pageGarBage
	pos += 2

	pageLastInsert := utils.MatchReadFrom2(d[pos:])
	logs.Debug("pageLastInsert:", pageLastInsert)
	page.ph.PAGE_LAST_INSERT = pageLastInsert
	pos += 2

	pageDirecTion := utils.MatchReadFrom2(d[pos:])
	logs.Debug("pageDirecTion:", pageDirecTion)
	page.ph.PAGE_DIRECTION = pageDirecTion
	pos += 2

	pageNDirecTion := utils.MatchReadFrom2(d[pos:])
	logs.Debug("pageNDirecTion:", pageNDirecTion)
	page.ph.PAGE_N_DIRECTION = pageNDirecTion
	pos += 2

	pageNRec := utils.MatchReadFrom2(d[pos:])
	logs.Debug("pageNRec:", pageNRec)
	page.ph.PAGE_N_RECS = pageNRec
	pos += 2

	pageMaxTrxID := utils.MatchReadFrom8(d[pos:])
	logs.Debug("pageMaxTrxID:", pageMaxTrxID)
	page.ph.PAGE_MAX_TRX_ID = pageMaxTrxID
	pos += 8

	pageLevel := utils.MatchReadFrom2(d[pos:])
	logs.Debug("pageLevel:", pageLevel)
	page.ph.PAGE_LEVEL = pageLevel
	pos += 2

	pageIndexId := utils.MatchReadFrom2(d[pos:])
	logs.Debug("pageIndexId:", pageIndexId)
	pos += 2

	page.data = d[pos:]

	return page
}

// Reference https://dev.mysql.com/doc/internals/en/innodb-page-overview.html
// An InnoDB page has seven parts:
// 1.Fil Header
// 2.Page Header
// 3.Infimum + Supremum Records
// 4.User Records
// 5.Free Space
// 6.Page Directory
// 7.Fil Trailer
// Read all those info and parse according to its protocol.
// TODO: support compress and encrypt page.
func (parse *Parse) parsePage(data []byte, pos int, columns []Columns, isRecovery bool, pageFree uint64) [][]Columns {

	// catch panic
	//defer func() {
	//	if err := recover(); err != nil {
	//		logs.Error("skip error, the error is ", err)
	//	}
	//}()

	var allColumns [][]Columns

	// The PAGE_BTR_SEG_LEAF length
	pos += 10

	// The PAGE_BTR_SEG_TOP length
	pos += 10

	// The inode len
	pos += 10

	// The supremum len
	pos += 26

	var infimum uint64
	var supremum uint64
	var offset uint64

	// MySQL row have many format, different formats correspond to different protocols
	// Reference https://dev.mysql.com/doc/refman/5.7/en/innodb-row-format.html
	if utils.PageIsCompact(data) == 0 {
		logs.Debug("row format is REDUNDANT")

		// mysql-5.7.19/storage/innobase/include/page0page.h
		// #define PAGE_OLD_INFIMUM	(PAGE_DATA + 1 + REC_N_OLD_EXTRA_BYTES)
		infimum = 38 + 36 + 2*10 + 1 + 6

		// mysql-5.7.19/storage/innobase/include/page0page.h
		// #define PAGE_OLD_SUPREMUM	(PAGE_DATA + 2 + 2 * REC_N_OLD_EXTRA_BYTES + 8)
		supremum = 38 + 36 + 2*10 + +2 + 2*6 + 8
	} else {
		logs.Debug("row format is COMPACT")

		// Reference mysql-5.7.19/storage/innobase/include/page0page.h
		// #define PAGE_DATA	(PAGE_HEADER + 36 + 2 * FSEG_HEADER_SIZE)
		// #define PAGE_NEW_INFIMUM	(PAGE_DATA + REC_N_NEW_EXTRA_BYTES)
		infimum = 38 + 36 + 2*10 + 5

		// #define PAGE_NEW_SUPREMUM	(PAGE_DATA + 2 * REC_N_NEW_EXTRA_BYTES + 8)
		supremum = 38 + 36 + 2*10 + 2*5 + 8
	}

	b := utils.MatchReadFrom2(data[infimum-2:])

	// Check the isRecovery to determine whether read normal data or page free data.
	// The page free data is deleted record, read it to recovery data.
	if isRecovery {
		offset = pageFree
	} else {
		if utils.PageIsCompact(data) == 0 {
			offset = b
		} else {
			offset = infimum + b
		}
	}

	var totalLen uint64
	for {
		// TODO: const
		if offset < (16384-6) && (offset != supremum) {
			var c []Columns
			logs.Debug("offset is ", offset, " supremum is ", supremum)

			// TODO: confirm the array length
			var offsets []uint64 = make([]uint64, len(columns)*2, len(columns)*2)
			origin := data[offset:]

			// Parse the row offset array, use the offset array can get
			// the column offset and len in the row.
			if utils.PageIsCompact(data) == 0 {
				parse.ibRecInitOffsetsOld(data, offset, origin, &offsets, uint64(len(columns)))
			} else {
				v, ok := parse.TableMap[columns[0].TableID]
				if !ok {
					logs.Error("can't find table by field's table id ", columns[0].TableID)
					break
				}
				table := v
				InitOffset := parse.ibRecInitOffsetsNew(data, offset, origin, &offsets, table)
				if !InitOffset {
					goto END
				}
			}

			//for i, value := range offsets {
			//	fmt.Println(i, value)
			//}

			if !parse.checkFieldSize(&offsets, columns) {
				break
			}

			c, _ = parse.parseRecords(data, origin, offsets, columns)
			totalLen += utils.RecOffsSize(&offsets)
			allColumns = append(allColumns, c)

		END:
			// Read the next record, the last 2 bytes store the next page offset.
			b = utils.MatchReadFrom2(data[(offset - 2):])
			if b == uint64(0) {
				break
			}

			// TODO: const
			if b > 32768 {
				b = b - 65536
			}

			if utils.PageIsCompact(data) != 0 {
				// TODO: get the deleted record may be the online record,
				//  When Page had split, the original record will be delete
				//  and insert into page free list.
				offset += b
			} else {
				offset = b
			}
		} else {
			// TODO: const
			PageID := utils.MatchReadFrom2(data[(38 + 24):])
			logs.Debug("PageID is ", PageID)
			break
		}
	}
	return allColumns
}

// Parse cluster index leaf page records, it corresponds to a row of data in the table.
// Reference https://dev.mysql.com/doc/internals/en/innodb-overview.html
func (parse *Parse) parseRecords(d []byte, o []byte, offsets []uint64, columns []Columns) ([]Columns, uint64) {

	var fieldLen uint64
	for i := 0; i < len(columns); i++ {
		// Get field len from offset array.
		data := utils.RecGetNthField(o, offsets, i, &fieldLen)
		if uint64(len(data)) < fieldLen {
			if fieldLen == 0xFFFFFFFF {
				columns[i].FieldValue = "NULL"
			}
			continue
		}

		// Parse the page record.
		value, err := utils.ParseData(columns[i].FieldType, columns[i].MySQLType, data, fieldLen,
			int(utils.GetFixedLength(columns[i].FieldType, columns[i].FieldLen)),
			columns[i].IsUnsigned, &columns[i].IsBinary)
		if err != nil {
			logs.Error(err.Error())
		}

		columns[i].FieldValue = value
	}

	var c = make([]Columns, len(columns))
	copy(c, columns)
	dataLen := utils.RecOffsDataSize(&offsets)
	return c, dataLen
}

// Make the sys tables column info
// Reference mysql-5.7.19/storage/innobase/dict/dict0boot.cc
func (parse *Parse) makeSysTablesColumns() []Columns {

	var columns []Columns
	columns = append(columns, Columns{FieldName: "NAME", FieldType: 4, FieldPos: 0, FieldLen: 0})
	columns = append(columns, Columns{FieldName: "DB_TRX_ID", FieldType: 1, FieldPos: 1, FieldLen: 6})
	columns = append(columns, Columns{FieldName: "DB_ROLL_PTR", FieldType: 1, FieldPos: 2, FieldLen: 7})
	columns = append(columns, Columns{FieldName: "ID", FieldType: 4, FieldPos: 3, FieldLen: 8})
	columns = append(columns, Columns{FieldName: "N_COLS", FieldType: 6, FieldPos: 4, FieldLen: 4})
	columns = append(columns, Columns{FieldName: "TYPE", FieldType: 6, FieldPos: 5, FieldLen: 4, IsUnsigned: true})
	columns = append(columns, Columns{FieldName: "MIX_ID", FieldType: 4, FieldPos: 6, FieldLen: 0})
	columns = append(columns, Columns{FieldName: "MIX_LEN", FieldType: 6, FieldPos: 7, FieldLen: 4, IsUnsigned: true})
	columns = append(columns, Columns{FieldName: "CLUSTER_NAME", FieldType: 4, FieldPos: 8, FieldLen: 0})
	columns = append(columns, Columns{FieldName: "SPACE", FieldType: 6, FieldPos: 9, FieldLen: 4, IsUnsigned: true})

	return columns
}

// Make the sys columns table column info
// Reference mysql-5.7.19/storage/innobase/dict/dict0boot.cc
func (parse *Parse) makeSysColumnsColumns() []Columns {

	var columns []Columns
	columns = append(columns, Columns{FieldName: "TABLE_ID", FieldType: 4, FieldPos: 0, FieldLen: 8})
	columns = append(columns, Columns{FieldName: "POS", FieldType: 6, FieldPos: 1, FieldLen: 4, IsUnsigned: true})
	columns = append(columns, Columns{FieldName: "DB_TRX_ID", FieldType: 1, FieldPos: 2, FieldLen: 6})
	columns = append(columns, Columns{FieldName: "DB_ROLL_PTR", FieldType: 1, FieldPos: 3, FieldLen: 7})
	columns = append(columns, Columns{FieldName: "NAME", FieldType: 4, FieldPos: 4, FieldLen: 0})
	columns = append(columns, Columns{FieldName: "MTYPE", FieldType: 6, FieldPos: 5, FieldLen: 4, IsUnsigned: true})
	columns = append(columns, Columns{FieldName: "PRTYPE", FieldType: 6, FieldPos: 6, FieldLen: 4, IsUnsigned: true})
	columns = append(columns, Columns{FieldName: "LEN", FieldType: 6, FieldPos: 7, FieldLen: 4, IsUnsigned: true})
	columns = append(columns, Columns{FieldName: "PREC", FieldType: 6, FieldPos: 8, FieldLen: 4, IsUnsigned: true})

	return columns
}

// Make the sys indexes table column info
// Reference mysql-5.7.19/storage/innobase/dict/dict0boot.cc
func (parse *Parse) makeSysIndexesColumns() []Columns {

	var columns []Columns
	columns = append(columns, Columns{FieldName: "TABLE_ID", FieldType: 4, FieldPos: 0, FieldLen: 8})
	columns = append(columns, Columns{FieldName: "ID", FieldType: 4, FieldPos: 1, FieldLen: 8})
	columns = append(columns, Columns{FieldName: "DB_TRX_ID", FieldType: 1, FieldPos: 2, FieldLen: 6})
	columns = append(columns, Columns{FieldName: "DB_ROLL_PTR", FieldType: 1, FieldPos: 3, FieldLen: 7})
	columns = append(columns, Columns{FieldName: "NAME", FieldType: 4, FieldPos: 4, FieldLen: 0})
	columns = append(columns, Columns{FieldName: "N_FIELDS", FieldType: 6, FieldPos: 5, FieldLen: 4, IsUnsigned: true})
	columns = append(columns, Columns{FieldName: "TYPE", FieldType: 6, FieldPos: 6, FieldLen: 4, IsUnsigned: true})
	columns = append(columns, Columns{FieldName: "SPACE", FieldType: 6, FieldPos: 7, FieldLen: 4, IsUnsigned: true})
	columns = append(columns, Columns{FieldName: "PAGE_NO", FieldType: 6, FieldPos: 8, FieldLen: 4, IsUnsigned: true})

	return columns
}

// Make the sys fields table column info
// Reference mysql-5.7.19/storage/innobase/dict/dict0boot.cc
func (parse *Parse) makeSysFieldsColumns() []Columns {

	var columns []Columns
	columns = append(columns, Columns{FieldName: "INDEX_ID", FieldType: 4, FieldPos: 0, FieldLen: 8})
	columns = append(columns, Columns{FieldName: "POS", FieldType: 6, FieldPos: 1, FieldLen: 4, IsUnsigned: true})
	columns = append(columns, Columns{FieldName: "DB_TRX_ID", FieldType: 1, FieldPos: 2, FieldLen: 6})
	columns = append(columns, Columns{FieldName: "DB_ROLL_PTR", FieldType: 1, FieldPos: 3, FieldLen: 7})
	columns = append(columns, Columns{FieldName: "COL_NAME", FieldType: 4, FieldPos: 4, FieldLen: 0})

	return columns
}

func (parse *Parse) checkFieldSize(offsets *[]uint64, columns []Columns) bool {
	for i := 0; i < len(columns); i++ {
		if utils.GetFixedLength(columns[i].FieldType, columns[i].FieldLen) != 0 {
			dataLen := utils.RecOffsNthSize(offsets, i)
			if dataLen == 0 && columns[i].IsNUll {
				continue
			}

			if columns[i].FieldLen != 0 && dataLen != utils.GetFixedLength(columns[i].FieldType, columns[i].FieldLen) {
				logs.Error("len is not equal field len,", "len is ", dataLen, " field len is ",
					utils.GetFixedLength(columns[i].FieldType, columns[i].FieldLen), " i is ", i, columns[i].FieldName)
				return false
			}
		}
	}
	return true
}

func (parse *Parse) getTableColumnsFromDict(dbName string, tableName string) ([]Columns, error) {
	var columns []Columns
	for _, table := range parse.TableMap {
		if table.DBName == dbName && table.TableName == tableName {
			columns = table.Columns
		}
	}
	return columns, nil
}

func (parse *Parse) getTableFromDict(dbName string, tableName string) (Tables, error) {
	var table Tables
	for _, t := range parse.TableMap {
		if t.DBName == dbName && t.TableName == tableName {
			table = t
		}
	}
	return table, nil
}

// reference /root/mysql-5.6.30/storage/innobase/include/rem0rec.ic
// rec_init_offsets
// When MySQL innodb Storage use REDUNDANT row format, use this method
// to calculate offset array which store column start offset and length.
func (parse *Parse) ibRecInitOffsetsOld(d []byte, offset uint64, o []byte, offsets *[]uint64, fieldNum uint64) {

	(*offsets)[2:][0] = 0
	(*offsets)[1] = fieldNum

	var offs uint64
	offs = 6
	num := utils.RecGet1byteOffsFlag(d, offset)

	if num != 0 {
		offs += fieldNum
		(*offsets)[2] = uint64(offs)

		var i uint64
		for i = 0; i < fieldNum; i++ {
			offs = utils.Rec1GetFieldEndInfo(d, uint64(len(d)-len(o)), uint64(i))
			// REC_1BYTE_SQL_NULL_MASK
			if (offs & 0x80) != 0 {
				offs = uint64(int(offs) & ^0x80)
				// #define REC_OFFS_SQL_NULL	((ulint) 1 << 31)
				offs |= 1 << 31
			}
			offs &= 0xffff

			// #define rec_offs_base(offsets) (offsets + REC_OFFS_HEADER_SIZE)
			(*offsets)[2:][1+i] = offs
		}
	} else {
		offs += 2 * fieldNum
		(*offsets)[2] = uint64(offs)
		var i uint64
		for i = 0; i < fieldNum; i++ {
			offs = utils.Rec2GetFieldEndInfo(d, uint64(len(d)-len(o)), uint64(i))
			if (offs & 0x8000) != 0 {
				// offs &= ~REC_2BYTE_SQL_NULL_MASK
				offs = uint64(int(offs) & ^0x8000)
				// #define REC_OFFS_SQL_NULL	((ulint) 1 << 31)
				offs = offs | (1 << 31)
			}

			// #define REC_2BYTE_EXTERN_MASK	0x4000UL
			if (offs & 0x4000) != 0 {
				// #define REC_2BYTE_EXTERN_MASK	0x4000UL
				offs = uint64(int(offs) & ^0x8000)
				// #define REC_OFFS_EXTERNAL	((ulint) 1 << 30)
				offs = offs | (1 << 30)
			}
			offs &= 0xffff
			(*offsets)[2:][1+i] = offs
		}
	}
}

// When MySQL innodb Storage use COMPACT row format, use this method
// to calculate offset array which store column start offset and length.
func (parse *Parse) ibRecInitOffsetsNew(d []byte, offset uint64, o []byte, offsets *[]uint64, table Tables) bool {

	(*offsets)[2:][0] = 0
	// TODO: maybe three internal fields, the rowid?
	// have two internal fields.
	(*offsets)[1] = uint64(len(table.Columns))

	var offs uint64
	// nulls = rec - (REC_N_NEW_EXTRA_BYTES + 1);
	nulls := d[(len(d) - len(o) - (5 + 1)):]
	lens := d[(len(d) - len(nulls) - (table.NullCount+7)/8):]
	offs = 0
	nullMask := 1
	for i := 0; i < len(table.Columns); i++ {
		var length uint64
		if table.Columns[i].IsNUll {

			if byte(nullMask) == 0 {
				// TODO: confirm
				nulls = d[(len(d) - len(nulls) - 1):]
				nullMask = 1
			}

			if (nulls[0] & byte(nullMask)) != 0 {
				nullMask <<= 1
				// /* SQL NULL flag in offsets returned by rec_get_offsets() */
				// #define REC_OFFS_SQL_NULL	((ulint) 1 << 31)
				length = offs | (1 << 31)
				goto OUT
			}
			nullMask <<= 1
		}

		if utils.GetFixedLength(table.Columns[i].FieldType, table.Columns[i].FieldLen) == 0 {
			length = uint64(lens[0])
			lens = d[(len(d) - len(lens) - 1):]
			//      if (debug) printf("Variable-length field: read the length\n");
			/* Variable-length field: read the length */

			if table.Columns[i].FieldLen > 255 || table.Columns[i].FieldType == utils.DATA_BLOB {

				if length&0x80 != 0 {

					// /* 1exxxxxxx xxxxxxxx */
					length <<= 8

					TempLength := uint64(lens[0])
					lens = d[(len(d) - len(lens) - 1):]
					length |= TempLength

					// length = length | uint64(d[(len(d) - len(lens) - 1):][0])
					offs += length & 0x3fff
					if (length & 0x4000) != 0 {
						// /* External flag in offsets returned by rec_get_offsets() */
						// #define REC_OFFS_EXTERNAL	((ulint) 1 << 30)
						length = offs | (1 << 30)
					} else {
						length = offs
					}
					goto OUT
				}
			}
			offs += length
			length = offs
		} else {
			offs += utils.GetFixedLength(table.Columns[i].FieldType, table.Columns[i].FieldLen)
			length = offs
		}
	OUT:
		offs &= 0xffff
		(*offsets)[2:][1+i] = length
	}
	return true
}
