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
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/zbdba/db-recovery/recovery/utils"
	"github.com/zbdba/db-recovery/recovery/utils/logs"
)

type ParseIB struct {
	// store table's struct.
	TableMap map[uint64]Tables
	// TODO: change to map. store page data.
	D *sync.Map
}

func NewParseIB() *ParseIB {
	return &ParseIB{
		TableMap: make(map[uint64]Tables),
		D:        new(sync.Map),
	}
}

// Parse the data file, The data file is composed of multiple pages,
// each page is 16kb by default.
func (p *ParseIB) ParseFile(path string) ([]Page, error) {

	var AllPages []Page
	file, err := os.Open(path)
	if err != nil {
		logs.Error("Error while opening file, the err is ", err)
		return nil, err
	}

	defer file.Close()

	var PageMap map[uint64]string
	PageMap = make(map[uint64]string)

	for {
		// Read a page from data file.
		d, err := utils.ReadNextBytes(file, DefaultPageSize)
		if len(d) < DefaultPageSize {
			break
		}
		if err != nil {
			logs.Error("read data from file failed, the error is %s", err.Error())
			return nil, err
		}

		// Parse the page file header.
		p, err := p.ParseFilHeader(d)
		if err != nil {
			return nil, err
		}

		// Store the page.
		p.OriginalData = d

		// TODO: why have the same pages?
		// Store the page into the PageMap.
		_, ok := PageMap[p.fh.FIL_PAGE_OFFSET]
		if !ok {
			AllPages = append(AllPages, p)
			PageMap[p.fh.FIL_PAGE_OFFSET] = "have"
		}
	}
	return AllPages, nil
}

func SliceInsert(c []Columns, index int, value Columns) []Columns {
	rear := append([]Columns{}, c[index:]...)
	return append(append(c[:index], value), rear...)
}

// Sort the columns.
func (p *ParseIB) SortColumns() {

	var SortColumns []Columns
	for _, table := range p.TableMap {
		for i, field := range table.Columns {
			if field.FieldPos == uint64(i) {
				SortColumns = append(SortColumns, field)
			}
		}
		table.Columns = SortColumns
	}

}

// Move the primary key field to the first.
func (p *ParseIB) MovePrimaryKeyFirst() {

	for TableId, table := range p.TableMap {
		for _, idx := range table.Indexes {
			if idx.Name == "PRIMARY" {
				table.Columns = p.MoveArrayElement(idx.Fields, table.Columns)
			}
		}
		p.TableMap[TableId] = table
	}
}

func (p *ParseIB) MoveArrayElement(fields []*Fields, columns []Columns) []Columns {

	for i := 0; i < len(columns); i++ {
		for j, field := range fields {
			if field.ColumnName == columns[i].FieldName {
				TempField := columns[i]
				columns = append(columns[:j], columns[j+1:]...)
				columns = append([]Columns{TempField}, columns...)
			}
		}
	}
	return columns
}

// Table have some internal filed in MySQL, such as DB_TRX_ID/DB_ROLL_PTR/DB_ROW_ID.
// When we parse a table row, we should add this internal columns.
// If table have primary key,there should have two internal fields: DB_TRX_ID, DB_ROLL_PTR.
// otherwise there should have three internal fields:DB_ROW_ID, DB_TRX_ID, DB_ROLL_PTR.
func (p *ParseIB) AddInternalColumns() {
	for TableId, table := range p.TableMap {
		for _, idx := range table.Indexes {
			if idx.Name == "PRIMARY" {
				// have primary key, add DB_TRX_ID, DB_ROLL_PTR after primary field.
				TrxColumns := p.AddInternalColumnsLow("DB_TRX_ID", TableId, idx.FieldNum)
				RollPtrColumns := p.AddInternalColumnsLow("DB_ROLL_PTR", TableId, idx.FieldNum+1)

				table.Columns = SliceInsert(table.Columns, int(idx.FieldNum), TrxColumns)
				table.Columns = SliceInsert(table.Columns, int(idx.FieldNum+1), RollPtrColumns)
				p.TableMap[TableId] = table
			} else if idx.Name == "GEN_CLUST_INDEX" {
				// don't have primary key, should add DB_ROW_ID field.
				// TODO: remove repeat code.
				RowIdColumns := p.AddInternalColumnsLow("DB_ROW_ID", TableId, 0)
				TrxColumns := p.AddInternalColumnsLow("DB_TRX_ID", TableId, 1)
				RollPtrColumns := p.AddInternalColumnsLow("DB_ROLL_PTR", TableId, 2)

				table.Columns = SliceInsert(table.Columns, 0, RowIdColumns)
				table.Columns = SliceInsert(table.Columns, 1, TrxColumns)
				table.Columns = SliceInsert(table.Columns, 2, RollPtrColumns)
				p.TableMap[TableId] = table
			}
		}
	}
}

func (p *ParseIB) AddInternalColumnsLow(InternalFieldName string, TableId uint64, FieldPos uint64) Columns {

	var InternalField Columns
	switch InternalFieldName {
	case "DB_ROW_ID":
		InternalField = Columns{FieldName: "DB_ROW_ID", FieldType: utils.DATA_MISSING, FieldPos: 0,
			FieldLen: 6, IsNUll: false, TableID: TableId}
	case "DB_TRX_ID":
		InternalField = Columns{FieldName: "DB_TRX_ID", FieldType: utils.DATA_MISSING,
			FieldPos: FieldPos, FieldLen: 6, IsNUll: false, TableID: TableId}
	case "DB_ROLL_PTR":
		InternalField = Columns{FieldName: "DB_ROLL_PTR", FieldType: utils.DATA_MISSING,
			FieldPos: FieldPos, FieldLen: 7, IsNUll: false, TableID: TableId}
	}
	return InternalField
}

// Get all column info from sys column dict tables.
func (p *ParseIB) GetAllColumns() error {

	var AllPageColumns [][]Columns
	columns := p.MakeSysColumnsColumns()
	v, ok := p.D.Load(SysColumnsIdx)
	if !ok {
		ErrMsg := fmt.Sprintf("sys column's page have not found")
		logs.Error(ErrMsg)
		return fmt.Errorf(ErrMsg)

	}

	dts := v.([]DataDict)
	for _, dt := range dts {

		// Start parse sys column table page.
		AllColumns := p.ParsePage(dt.data, dt.pos, columns, false, 0)
		AllPageColumns = append(AllPageColumns, AllColumns...)
	}

	for _, c := range AllPageColumns {

		v, ok := p.TableMap[c[0].FieldValue.(uint64)]
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

			p.TableMap[c[0].FieldValue.(uint64)] = t
		}
	}
	return nil
}

// Get all index info from sys index dict table.
func (p *ParseIB) GetAllIndexes() error {

	var AllPageColumns [][]Columns
	columns := p.MakeSysIndexesColumns()
	v, ok := p.D.Load(SysIndexesIdx)
	if !ok {
		ErrMsg := fmt.Sprintf("sys indexs's page have not found")
		logs.Error(ErrMsg)
		return fmt.Errorf(ErrMsg)
	}

	dts := v.([]DataDict)
	for _, dt := range dts {
		// Start parse sys indexes table pages.
		AllColumns := p.ParsePage(dt.data, dt.pos, columns, false, 0)
		AllPageColumns = append(AllPageColumns, AllColumns...)

	}

	for _, columns := range AllPageColumns {
		var index map[uint64]Indexes
		v, ok := p.TableMap[columns[0].FieldValue.(uint64)]

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
			p.TableMap[columns[0].FieldValue.(uint64)] = t
		} else {
			logs.Error("Table ID have not found ", columns[0].FieldValue)
		}
	}
	return nil
}

// Get all fields from sys fields dict table.
func (p *ParseIB) GetAllFields() error {

	var AllPageColumns [][]Columns
	columns := p.MakeSysFieldsColumns()
	v, ok := p.D.Load(SysFieldsIdx)
	if !ok {
		ErrMsg := fmt.Sprintf("sys field's page have not found")
		logs.Error(ErrMsg)
		return fmt.Errorf(ErrMsg)
	}

	dts := v.([]DataDict)
	for _, dt := range dts {
		// Start parse sys fields table pages.
		AllColumns := p.ParsePage(dt.data, dt.pos, columns, false, 0)
		AllPageColumns = append(AllPageColumns, AllColumns...)
	}

	// store all fields into index map
	IndexFieldsMap := make(map[uint64][]*Fields)
	for _, columns := range AllPageColumns {
		v, ok := IndexFieldsMap[columns[0].FieldValue.(uint64)]
		if ok {
			v = append(v, &Fields{ColumnPos: columns[1].FieldValue.(uint64),
				ColumnName: columns[4].FieldValue.(string)})
			IndexFieldsMap[columns[0].FieldValue.(uint64)] = v
		} else {
			// TODO: nil value
			var fields []*Fields
			// TODO: Column Type
			fields = append(fields, &Fields{ColumnPos: columns[1].FieldValue.(uint64),
				ColumnName: columns[4].FieldValue.(string)})
			IndexFieldsMap[columns[0].FieldValue.(uint64)] = fields
		}
	}

	// Scan all table index and get fields from index map
	for TableId, table := range p.TableMap {
		// Scan table index's array.
		for IndexId, idx := range table.Indexes {
			v, ok := IndexFieldsMap[IndexId]
			if ok {
				// TODO: confirm, should store map again?
				idx.Fields = v
			}
			table.Indexes[IndexId] = idx
		}
		p.TableMap[TableId] = table
	}

	return nil
}

// Get all table info from sys dict tables.
func (p *ParseIB) GetAllTables() error {

	columns := p.MakeSysTablesColumns()
	v, ok := p.D.Load(SysTablesIdx)
	if !ok {
		ErrMsg := fmt.Sprintf("sys table's page have not found")
		logs.Error(ErrMsg)
		return fmt.Errorf(ErrMsg)
	}

	dts := v.([]DataDict)
	for _, dt := range dts {

		// parse sys table page.
		AllColumns := p.ParsePage(dt.data, dt.pos, columns, false, 0)

		for _, columns := range AllColumns {
			// database and table name, for example: zbdba3/jingbo_test
			var TBName string
			var DBName string
			if strings.Contains(columns[0].FieldValue.(string), "/") {
				n := strings.Split(columns[0].FieldValue.(string), "/")
				DBName = n[0]
				TBName = n[1]
			} else {
				TBName = columns[0].FieldValue.(string)
			}
			p.TableMap[columns[3].FieldValue.(uint64)] =
				Tables{
					DBName:    DBName,
					TableName: TBName,
					NullCount: 0,
					SpaceId:   columns[9].FieldValue.(uint64)}
		}
	}
	return nil
}

// Get table struct in dict page.
func (p *ParseIB) ParseDictPage(FilePath string) error {

	pages, err := p.ParseFile(FilePath)
	if err != nil {
		logs.Error("parse system data file failed, the error is ", err)
		return err
	}

	var SystemPage Page
	for _, page := range pages {
		if page.fh.FIL_PAGE_OFFSET == SystemPageIdx {
			SystemPage, err = p.ParseSysPageHeader(page)
			if err != nil {
				return err
			}
			break
		}
	}

	// Get all dict pages, and use map to store all dict page's data.
	for _, page := range pages {

		// TODO: Parse undo page from sys data file.
		//if p.fh.FIL_PAGE_TYPE == uint64(2) {
		//	P.ParseUndoPageHeader(&p)
		//}

		if page.fh.FIL_PAGE_OFFSET == SystemPage.sp.DICT_HDR_TABLES ||
			page.fh.FIL_PAGE_OFFSET == SystemPage.sp.DICT_HDR_COLUMNS ||
			page.fh.FIL_PAGE_OFFSET == SystemPage.sp.DICT_HDR_INDEXES ||
			page.fh.FIL_PAGE_OFFSET == SystemPage.sp.DICT_HDR_FIELDS {

			var ds []DataDict
			p.ParsePageHeader(&page)
			ds = append(ds,
				DataDict{
					IndexId:    page.ph.PAGE_INDEX_ID,
					PageOffset: page.fh.FIL_PAGE_OFFSET,
					data:       page.OriginalData,
					pos:        len(page.OriginalData) - len(page.data)})
			p.D.Store(page.ph.PAGE_INDEX_ID, ds)

			logs.Debug("page offset is ", page.fh.FIL_PAGE_OFFSET,
				"indexId is", page.ph.PAGE_INDEX_ID, " data len is ", len(ds))

		} else {
			f := func(k, v interface{}) bool {
				if k == page.ph.PAGE_INDEX_ID {
					ds := v.([]DataDict)
					p.ParsePageHeader(&page)
					ds = append(ds,
						DataDict{
							IndexId:    page.ph.PAGE_INDEX_ID,
							PageOffset: page.fh.FIL_PAGE_OFFSET,
							data:       page.OriginalData,
							pos:        len(page.OriginalData) - len(page.data)})
					p.D.Store(page.ph.PAGE_INDEX_ID, ds)

					logs.Debug("page offset is ", page.fh.FIL_PAGE_OFFSET,
						"indexId is", page.ph.PAGE_INDEX_ID, " data len is ", len(ds))
				}
				return true
			}
			p.D.Range(f)
		}
	}

	logs.Debug("start parse sys_tables.")
	GetTableErr := p.GetAllTables()
	if GetTableErr != nil {
		return GetTableErr
	}

	logs.Debug("start parse sys_columns.")
	GetColumnErr := p.GetAllColumns()
	if GetColumnErr != nil {
		return GetTableErr
	}

	logs.Debug("start parse sys_index.")
	GetIndexesErr := p.GetAllIndexes()
	if GetIndexesErr != nil {
		return GetIndexesErr
	}

	logs.Debug("start parse sys_fields.")
	GetFieldsErr := p.GetAllFields()
	if GetFieldsErr != nil {
		return GetFieldsErr
	}

	// Sort fields
	// TODO: why some table fields is not sort?
	p.SortColumns()

	// Move primary key first.
	p.MovePrimaryKeyFirst()

	// Add internal columns.
	p.AddInternalColumns()

	return nil
}

func (p *ParseIB) ParseSysPageHeader(page Page) (Page, error) {

	pos := 0
	d := page.data

	DictHdrRowId := utils.MatchReadFrom8(d[pos:])
	pos += 8
	logs.Debug("DictHdrRowId:", DictHdrRowId)
	page.sp.DICT_HDR_ROW_ID = DictHdrRowId

	DictHdrTableId := utils.MatchReadFrom8(d[pos:])
	pos += 8
	logs.Debug("DictHdrTableId:", DictHdrTableId)
	page.sp.DICT_HDR_TABLE_ID = DictHdrTableId

	DictHdrIndexId := utils.MatchReadFrom8(d[pos:])
	pos += 8
	logs.Debug("DictHdrIndexId:", DictHdrIndexId)
	page.sp.DICT_HDR_INDEX_ID = DictHdrIndexId

	DictHdrMaxSpaceId := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("DictHdrMaxSpaceId:", DictHdrMaxSpaceId)
	page.sp.DICT_HDR_MAX_SPACE_ID = DictHdrMaxSpaceId

	DictHdrMaxIdLow := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("DictHdrMaxIdLow:", DictHdrMaxIdLow)
	page.sp.DICT_HDR_MIX_ID_LOW = DictHdrMaxIdLow

	DictHdrTables := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("DictHdrTables:", DictHdrTables)
	page.sp.DICT_HDR_TABLES = DictHdrTables

	DictHdrTableIds := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("DictHdrTableIds:", DictHdrTableIds)
	page.sp.DICT_HDR_TABLE_IDS = DictHdrTableIds

	DictHdrColumns := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("DictHdrColumns:", DictHdrColumns)
	page.sp.DICT_HDR_COLUMNS = DictHdrColumns

	DictHdrIndexes := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("DictHdrIndexes:", DictHdrIndexes)
	page.sp.DICT_HDR_INDEXES = DictHdrIndexes

	DictHdrFields := utils.MatchReadFrom4(d[pos:])
	pos += 4
	logs.Debug("DictHdrFields:", DictHdrFields)
	page.sp.DICT_HDR_FIELDS = DictHdrFields

	return page, nil
}

func (p *ParseIB) ParseFilHeader(d []byte) (Page, error) {

	var page Page
	pos := 0

	// Get index id
	IndexId := utils.MatchReadFrom8(d[(pos + 38 + 28):])
	logs.Debug("Index is is ", IndexId)
	page.ph.PAGE_INDEX_ID = IndexId

	// Parse Fil Header
	SpaceId := utils.MatchReadFrom4(d[pos:])
	logs.Debug("SpaceId:", SpaceId)
	page.fh.FIL_PAGE_SPACE = SpaceId
	pos += 4

	PageOffset := utils.MatchReadFrom4(d[pos:])
	logs.Debug("PageOffset:", PageOffset)
	page.fh.FIL_PAGE_OFFSET = PageOffset
	pos += 4

	PagePrev := utils.MatchReadFrom4(d[pos:])
	logs.Debug("PagePrev:", PagePrev)
	page.fh.FIL_PAGE_PREV = PagePrev
	pos += 4

	PageNext := utils.MatchReadFrom4(d[pos:])
	logs.Debug("PagePrev:", PageNext)
	page.fh.FIL_PAGE_NEXT = PageNext
	pos += 4

	PageLsn := utils.MatchReadFrom8(d[pos:])
	logs.Debug("PageLsn:", PageLsn)
	page.fh.FIL_PAGE_LSN = PageLsn
	pos += 8

	PageType := utils.MatchReadFrom2(d[pos:])
	logs.Debug("PageType:", PageType)
	page.fh.FIL_PAGE_TYPE = PageType
	pos += 2

	PageFileFlushLsn := utils.MatchReadFrom8(d[pos:])
	logs.Debug("PageFileFlushLsn:", PageFileFlushLsn)
	page.fh.FIL_PAGE_LSN = PageFileFlushLsn
	pos += 8

	PageArchLogNo := utils.MatchReadFrom4(d[pos:])
	logs.Debug("PageArchLogNo:", PageArchLogNo)
	page.fh.FIL_PAGE_ARCH_LOG_NO = PageArchLogNo
	pos += 4

	page.data = d[pos:]

	return page, nil
}

// TODO: parse undo info from system data file.
func (p *ParseIB) ParseUndoPageHeader(page *Page) *Page {

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
	pos += 1

	TRX_UNDO_DICT_TRANS := utils.MatchReadFrom1(d[pos:])
	fmt.Println("the TRX_UNDO_DICT_TRANS is ", TRX_UNDO_DICT_TRANS)
	pos += 1

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
	tmp_pos += 1

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

func (p *ParseIB) ParsePageHeader(page *Page) *Page {

	d := page.data
	pos := 0

	PageNDirSlots := utils.MatchReadFrom2(d[pos:])
	logs.Debug("PageNDirSlots:", PageNDirSlots)
	page.ph.PAGE_N_DIR_SLOTS = PageNDirSlots
	pos += 2

	PageHeapTop := utils.MatchReadFrom2(d[pos:])
	logs.Debug("PageHeapTop:", PageHeapTop)
	page.ph.PAGE_HEAP_TOP = PageHeapTop
	pos += 2

	PageNHeap := utils.MatchReadFrom2(d[pos:])
	logs.Debug("PageNHeap:", PageNHeap)
	page.ph.PAGE_N_HEAP = PageNHeap
	pos += 2

	PageFree := utils.MatchReadFrom2(d[pos:])
	logs.Debug("PageFree:", PageFree)
	page.ph.PAGE_FREE = PageFree
	pos += 2

	PageGarBage := utils.MatchReadFrom2(d[pos:])
	logs.Debug("PageGarBage:", PageGarBage)
	page.ph.PAGE_GARBAGE = PageGarBage
	pos += 2

	PageLastInsert := utils.MatchReadFrom2(d[pos:])
	logs.Debug("PageLastInsert:", PageLastInsert)
	page.ph.PAGE_LAST_INSERT = PageLastInsert
	pos += 2

	PageDirecTion := utils.MatchReadFrom2(d[pos:])
	logs.Debug("PageDirecTion:", PageDirecTion)
	page.ph.PAGE_DIRECTION = PageDirecTion
	pos += 2

	PageNDirecTion := utils.MatchReadFrom2(d[pos:])
	logs.Debug("PageNDirecTion:", PageNDirecTion)
	page.ph.PAGE_N_DIRECTION = PageNDirecTion
	pos += 2

	PageNRec := utils.MatchReadFrom2(d[pos:])
	logs.Debug("PageNRec:", PageNRec)
	page.ph.PAGE_N_RECS = PageNRec
	pos += 2

	PageMaxTrxId := utils.MatchReadFrom8(d[pos:])
	logs.Debug("PageMaxTrxId:", PageMaxTrxId)
	page.ph.PAGE_MAX_TRX_ID = PageMaxTrxId
	pos += 8

	PageLevel := utils.MatchReadFrom2(d[pos:])
	logs.Debug("PageLevel:", PageLevel)
	page.ph.PAGE_LEVEL = PageLevel
	pos += 2

	PageIndexId := utils.MatchReadFrom2(d[pos:])
	logs.Debug("PageIndexId:", PageIndexId)
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
func (p *ParseIB) ParsePage(d []byte, pos int, columns []Columns, IsRecovery bool, PageFree uint64) [][]Columns {

	// catch panic
	//defer func() {
	//	if err := recover(); err != nil {
	//		logs.Error("skip error, the error is ", err)
	//	}
	//}()

	var AllColumns [][]Columns

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
	if utils.PageIsComp(d) == 0 {
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

	b := utils.MatchReadFrom2(d[infimum-2:])

	// Check the IsRecovery to determine whether read normal data or page free data.
	// The page free data is deleted record, read it to recovery data.
	if IsRecovery {
		offset = PageFree
	} else {
		if utils.PageIsComp(d) == 0 {
			offset = b
		} else {
			offset = infimum + b
		}
	}

	var TotalLen uint64
	for {
		// TODO: const
		if offset < (16384-6) && (offset != supremum) {
			var c []Columns
			logs.Debug("offset is ", offset, " supremum is ", supremum)

			// TODO: confirm the array length
			var offsets []uint64 = make([]uint64, len(columns)*2, len(columns)*2)
			origin := d[offset:]

			// Parse the row offset array, use the offset array can get
			// the column offset and len in the row.
			if utils.PageIsComp(d) == 0 {
				p.IbrecInitOffsetsOld(d, offset, origin, &offsets, uint64(len(columns)))
			} else {
				v, ok := p.TableMap[columns[0].TableID]
				if !ok {
					logs.Error("can't find table by field's table id ", columns[0].TableID)
					break
				}
				table := v
				InitOffset := p.IbrecInitOffsetsNew(d, offset, origin, &offsets, table)
				if !InitOffset {
					goto END
				}
			}

			//for i, value := range offsets {
			//	fmt.Println(i, value)
			//}

			if !p.CheckFieldSize(&offsets, columns) {
				break
			}

			c, _ = p.ParseRecords(d, origin, offsets, columns)
			TotalLen += utils.RecOffsSize(&offsets)
			AllColumns = append(AllColumns, c)

		END:
			// Read the next record, the last 2 bytes store the next page offset.
			b = utils.MatchReadFrom2(d[(offset - 2):])
			if b == uint64(0) {
				break
			}

			// TODO: const
			if b > 32768 {
				b = b - 65536
			}

			if utils.PageIsComp(d) != 0 {
				// TODO: get the deleted record may be the online record,
				//  When Page had split, the original record will be delete
				//  and insert into page free list.
				offset += b
			} else {
				offset = b
			}
		} else {
			// TODO: const
			PageID := utils.MatchReadFrom2(d[(38 + 24):])
			logs.Debug("PageID is ", PageID)
			break
		}
	}
	return AllColumns
}

// Parse cluster index leaf page records, it corresponds to a row of data in the table.
// Reference https://dev.mysql.com/doc/internals/en/innodb-overview.html
func (p *ParseIB) ParseRecords(d []byte, o []byte, offsets []uint64, columns []Columns) ([]Columns, uint64) {

	var FieldLen uint64
	for i := 0; i < len(columns); i++ {
		// Get field len from offset array.
		data := utils.RecGetNthField(o, offsets, i, &FieldLen)
		if uint64(len(data)) < FieldLen {
			if FieldLen == 0xFFFFFFFF {
				columns[i].FieldValue = "NULL"
			}
			continue
		}

		// Parse the page record.
		value, err := utils.ParseData(columns[i].FieldType, columns[i].MySQLType, data, FieldLen,
			int(utils.GetFixedLength(columns[i].FieldType, columns[i].FieldLen)),
			columns[i].IsUnsigned, &columns[i].IsBinary)
		if err != nil {
			logs.Error(err.Error())
		}

		columns[i].FieldValue = value
	}

	var c = make([]Columns, len(columns))
	copy(c, columns)
	DataLen := utils.RecOffsDataSize(&offsets)
	return c, DataLen
}

// Make the sys tables column info
// Reference mysql-5.7.19/storage/innobase/dict/dict0boot.cc
func (p *ParseIB) MakeSysTablesColumns() []Columns {

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
func (p *ParseIB) MakeSysColumnsColumns() []Columns {

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
func (p *ParseIB) MakeSysIndexesColumns() []Columns {

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
func (p *ParseIB) MakeSysFieldsColumns() []Columns {

	var columns []Columns
	columns = append(columns, Columns{FieldName: "INDEX_ID", FieldType: 4, FieldPos: 0, FieldLen: 8})
	columns = append(columns, Columns{FieldName: "POS", FieldType: 6, FieldPos: 1, FieldLen: 4, IsUnsigned: true})
	columns = append(columns, Columns{FieldName: "DB_TRX_ID", FieldType: 1, FieldPos: 2, FieldLen: 6})
	columns = append(columns, Columns{FieldName: "DB_ROLL_PTR", FieldType: 1, FieldPos: 3, FieldLen: 7})
	columns = append(columns, Columns{FieldName: "COL_NAME", FieldType: 4, FieldPos: 4, FieldLen: 0})

	return columns
}

func (p *ParseIB) CheckFieldSize(offsets *[]uint64, columns []Columns) bool {
	for i := 0; i < len(columns); i++ {
		if utils.GetFixedLength(columns[i].FieldType, columns[i].FieldLen) != 0 {
			DataLen := utils.RecOffsNthSize(offsets, i)
			if DataLen == 0 && columns[i].IsNUll {
				continue
			}

			if columns[i].FieldLen != 0 && DataLen != utils.GetFixedLength(columns[i].FieldType, columns[i].FieldLen) {
				logs.Error("len is not equal field len,", "len is ", DataLen, " field len is ",
					utils.GetFixedLength(columns[i].FieldType, columns[i].FieldLen), " i is ", i, columns[i].FieldName)
				return false
			}
		}
	}
	return true
}

func (p *ParseIB) GetColumnType(ColumnName string, columns []Columns) uint64 {
	for _, f := range columns {
		if f.FieldName == ColumnName {
			return f.FieldType
		}
	}
	return 0
}

// TODO: read table struct info use sql parse,
//  user should identify the create table sql statement.
func (p *ParseIB) GetTableFieldsFromStruct() {
	// TODO: need sql parser
}

func (p *ParseIB) GetTableColumnsFromDict(DBName string, TableName string) ([]Columns, error) {
	var columns []Columns
	for _, table := range p.TableMap {
		if table.DBName == DBName && table.TableName == TableName {
			columns = table.Columns
		}
	}
	return columns, nil
}

func (p *ParseIB) GetTableFromDict(DBName string, TableName string) (Tables, error) {
	var table Tables
	for _, t := range p.TableMap {
		if t.DBName == DBName && t.TableName == TableName {
			table = t
		}
	}
	return table, nil
}

// TODO: recovery table structure from system data dictionary.
func (p *ParseIB) RecoveryTableStruct(path string, DBName string, TableName string) (string, error) {
	_, ParseFileErr := p.ParseFile(path)
	if ParseFileErr != nil {
		return "", ParseFileErr
	}

	// Get table fields.
	table, GetFieldsErr := p.GetTableFromDict(DBName, TableName)
	if GetFieldsErr != nil {
		return "", GetFieldsErr
	}

	CreateTableSql := p.MakeCreateTableSql(table)

	return CreateTableSql, nil
}

// TODO: use column info to make create table sql statement.
func (p *ParseIB) MakeCreateTableSql(table Tables) string {
	return ""
}

// Read table data from data file, should identified the file
// path/database name/table name which can confirm the table info.
// You should identify the IsRecovery to confirm
// whether recovery table data or just read table data.
func (p *ParseIB) ParseTableData(path string, DBName string, TableName string, IsRecovery bool) error {
	pages, ParseFileErr := p.ParseFile(path)
	if ParseFileErr != nil {
		return ParseFileErr
	}

	// Get table fields info from data dict.
	fields, GetFieldsErr := p.GetTableColumnsFromDict(DBName, TableName)
	if GetFieldsErr != nil {
		logs.Error("get fields from dict failed, the error is ", GetFieldsErr,
			" the db name is ", DBName, " the table name is ", TableName)
		return GetFieldsErr
	}

	for _, page := range pages {

		logs.Debug("page.fh.FIL_PAGE_TYPE is ", page.fh.FIL_PAGE_TYPE,
			"page.fh.FIL_PAGE_OFFSET", page.fh.FIL_PAGE_OFFSET)

		p.ParsePageHeader(&page)

		if IsRecovery {
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
			!(IsRecovery && page.ph.PAGE_FREE == 0) {
			AllColumns := p.ParsePage(page.OriginalData, len(page.OriginalData)-len(page.data),
				fields, IsRecovery, page.ph.PAGE_FREE)
			if len(AllColumns) == 0 {
				logs.Info("have no data")
				continue
			}

			// Make replace into statement.
			p.MakeReplaceIntoStatement(AllColumns, TableName, DBName)
		}
	}
	return nil
}

// Make row data to replace into statement, it will be more convenient when restoring data.
func (p *ParseIB) MakeReplaceIntoStatement(AllColumns [][]Columns, table string, database string) {
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

// refrence /root/mysql-5.6.30/storage/innobase/include/rem0rec.ic
// rec_init_offsets
// When MySQL innodb Storage use REDUNDANT row format, use this method
// to calculate offset array which store column start offset and length.
func (p *ParseIB) IbrecInitOffsetsOld(d []byte, offset uint64, o []byte, offsets *[]uint64, FieldNum uint64) {

	(*offsets)[2:][0] = 0
	(*offsets)[1] = FieldNum

	var offs uint64
	offs = 6
	num := utils.RecGet1byteOffsFlag(d, offset)

	if num != 0 {
		offs += FieldNum
		(*offsets)[2] = uint64(offs)

		var i uint64
		for i = 0; i < FieldNum; i++ {
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
		offs += 2 * FieldNum
		(*offsets)[2] = uint64(offs)
		var i uint64
		for i = 0; i < FieldNum; i++ {
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
func (p *ParseIB) IbrecInitOffsetsNew(d []byte, offset uint64, o []byte, offsets *[]uint64, table Tables) bool {

	(*offsets)[2:][0] = 0
	// TODO: maybe three internal fields, the rowid?
	// have two internal fields.
	(*offsets)[1] = uint64(len(table.Columns))

	var offs uint64
	// nulls = rec - (REC_N_NEW_EXTRA_BYTES + 1);
	nulls := d[(len(d) - len(o) - (5 + 1)):]
	lens := d[(len(d) - len(nulls) - (table.NullCount+7)/8):]
	offs = 0
	NullMask := 1
	for i := 0; i < len(table.Columns); i++ {
		var length uint64
		if table.Columns[i].IsNUll {

			if byte(NullMask) == 0 {
				// TODO: confirm
				nulls = d[(len(d) - len(nulls) - 1):]
				NullMask = 1
			}

			if (nulls[0] & byte(NullMask)) != 0 {
				NullMask <<= 1
				// /* SQL NULL flag in offsets returned by rec_get_offsets() */
				// #define REC_OFFS_SQL_NULL	((ulint) 1 << 31)
				length = offs | (1 << 31)
				goto OUT
			}
			NullMask <<= 1
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
