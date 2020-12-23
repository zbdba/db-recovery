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

// Tables store the table structure info.
type Tables struct {
	DBName    string
	TableName string
	Columns   []Columns
	Indexes   map[uint64]Indexes
	NullCount int
	SpaceId   uint64
}

// Store the table columns info.
type Columns struct {
	FieldName  string
	FieldType  uint64
	MySQLType  uint64
	FieldPos   uint64
	FieldLen   uint64
	FieldValue interface{}
	IsNUll     bool
	IsBinary   bool
	IsUnsigned bool
	TableID    uint64
}

// Store the table index info.
type Indexes struct {
	Id        uint64
	Name      string
	FieldNum  uint64
	IndexType uint64
	Fields    []*Fields
}

// Store the fields of the table index.
type Fields struct {
	ColumnPos  uint64
	ColumnName string
	//ColumnType  uint64
	ColumnValue interface{}
}

// Store the data dictionary info.
type DataDict struct {
	IndexId    uint64
	PageOffset uint64
	data       []byte
	pos        int
}

// Name those fields with MySQL code style.
// It Store the MySQL page file header.
// Reference https://dev.mysql.com/doc/internals/en/innodb-fil-header.html
type FilHeader struct {
	FIL_PAGE_SPACE          uint64
	FIL_PAGE_OFFSET         uint64
	FIL_PAGE_PREV           uint64
	FIL_PAGE_NEXT           uint64
	FIL_PAGE_LSN            uint64
	FIL_PAGE_TYPE           uint64
	FIL_PAGE_FILE_FLUSH_LSN uint64
	FIL_PAGE_ARCH_LOG_NO    uint64
}

// Name those fields with MySQL code style.
// It Store the MySQL page header, only the index page have this header.
// Reference https://dev.mysql.com/doc/internals/en/innodb-page-header.html
type PageHeader struct {
	PAGE_N_DIR_SLOTS  uint64
	PAGE_HEAP_TOP     uint64
	PAGE_N_HEAP       uint64
	PAGE_FREE         uint64
	PAGE_GARBAGE      uint64
	PAGE_LAST_INSERT  uint64
	PAGE_DIRECTION    uint64
	PAGE_N_DIRECTION  uint64
	PAGE_N_RECS       uint64
	PAGE_MAX_TRX_ID   uint64
	PAGE_LEVEL        uint64
	PAGE_INDEX_ID     uint64
	PAGE_BTR_SEG_LEAF uint64
	PAGE_BTR_SEG_TOP  uint64
}

// Name those fields with MySQL code style.
// It Store the MySQL system page header, only the system page have this header.
// Reference mysql-5.7.19/storage/innobase/include/dict0boot.h
type SystemPageHeader struct {
	DICT_HDR_ROW_ID       uint64
	DICT_HDR_TABLE_ID     uint64
	DICT_HDR_INDEX_ID     uint64
	DICT_HDR_MAX_SPACE_ID uint64
	DICT_HDR_MIX_ID_LOW   uint64
	DICT_HDR_TABLES       uint64
	DICT_HDR_TABLE_IDS    uint64
	DICT_HDR_COLUMNS      uint64
	DICT_HDR_INDEXES      uint64
	DICT_HDR_FIELDS       uint64
}

// Page ...
type Page struct {
	// All MySQL page have this header.
	fh FilHeader

	// Only the index page have this header.
	ph PageHeader

	// only system page have.
	sp SystemPageHeader

	// Store the page data.
	OriginalData []byte
	data         []byte
}
