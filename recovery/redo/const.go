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

// The MySQL Innodb redo log block size.
const (
	// storage/innobase/include/os0file.h
	// #define OS_FILE_LOG_BLOCK_SIZE 512
	OS_FILE_LOG_BLOCK_SIZE = 512
)

// The MySQL Innodb redo record type.
// storage/innobase/include/mtr0types.h
// unittest/gunit/innodb/lob/mtr0types.h
const (
	/** if the mtr contains only one log record for one page,
	  i.e., write_initial_log_record has been called only once,
	  this flag is ORed to the type of that first log record */
	MLOG_SINGLE_REC_FLAG = 128

	/** one byte is written */
	MLOG_1BYTE = 1

	/** 2 bytes ... */
	MLOG_2BYTES = 2

	/** 4 bytes ... */
	MLOG_4BYTES = 4

	/** 8 bytes ... */
	MLOG_8BYTES = 8

	/** Record insert */
	MLOG_REC_INSERT = 9

	/** Mark clustered index record deleted */
	MLOG_REC_CLUST_DELETE_MARK = 10

	/** Mark secondary index record deleted */
	MLOG_REC_SEC_DELETE_MARK = 11

	/** update of a record, preserves record field sizes */
	MLOG_REC_UPDATE_IN_PLACE = 13

	/*!< Delete a record from a page */
	MLOG_REC_DELETE = 14

	/** Delete record list end on index page */
	MLOG_LIST_END_DELETE = 15

	/** Delete record list start on index page */
	MLOG_LIST_START_DELETE = 16

	/** Copy record list end to a new created index page */
	MLOG_LIST_END_COPY_CREATED = 17
	/** Reorganize an index page in ROW_FORMAT=REDUNDANT */
	MLOG_PAGE_REORGANIZE = 18

	/** Create an index page */
	MLOG_PAGE_CREATE = 19

	/** Insert entry in an undo log */
	MLOG_UNDO_INSERT = 20

	/** erase an undo log page end */
	MLOG_UNDO_ERASE_END = 21

	/** initialize a page in an undo log */
	MLOG_UNDO_INIT = 22

	/* discard an update undo log header (unused already in 3.23.53) */
	MLOG_UNDO_HDR_DISCARD = 23

	/** reuse an insert undo log header */
	MLOG_UNDO_HDR_REUSE = 24

	/** create an undo log header */
	MLOG_UNDO_HDR_CREATE = 25

	/** mark an index record as the predefined minimum record */
	MLOG_REC_MIN_MARK = 26

	/** initialize an ibuf bitmap page */
	MLOG_IBUF_BITMAP_INIT = 27

	// #ifdef UNIV_LOG_LSN_DEBUG
	/** Current LSN */
	MLOG_LSN = 28
	// #endif /* UNIV_LOG_LSN_DEBUG */

	/** this means that a file page is taken into use and the prior
	  contents of the page should be ignored: in recovery we must not
	  trust the lsn values stored to the file page.
	  Note: it's deprecated because it causes crash recovery problem
	  in bulk create index, and actually we don't need to reset page
	  lsn in recv_recover_page_func() now. */
	MLOG_INIT_FILE_PAGE = 29

	/** write a string to a page */
	MLOG_WRITE_STRING = 30

	/** If a single mtr writes several log records, this log
	  record ends the sequence of these records */
	MLOG_MULTI_REC_END = 31

	/** dummy log record used to pad a log block full */
	MLOG_DUMMY_RECORD = 32

	/** log record about creating an .ibd file, with format */
	MLOG_FILE_CREATE = 33

	/** rename a tablespace file that starts with (space_id,page_no) */
	MLOG_FILE_RENAME = 34

	/** delete a tablespace file that starts with (space_id,page_no) */
	MLOG_FILE_DELETE = 35

	/** mark a compact index record as the predefined minimum record */
	MLOG_COMP_REC_MIN_MARK = 36

	/** create a compact index page */
	MLOG_COMP_PAGE_CREATE = 37

	/** compact record insert */
	MLOG_COMP_REC_INSERT = 38

	/** mark compact clustered index record deleted */
	MLOG_COMP_REC_CLUST_DELETE_MARK = 39

	/** mark compact secondary index record deleted; this log
	  record type is redundant, as MLOG_REC_SEC_DELETE_MARK is
	  independent of the record format. */
	MLOG_COMP_REC_SEC_DELETE_MARK = 40

	/** update of a compact record, preserves record field sizes */
	MLOG_COMP_REC_UPDATE_IN_PLACE = 41

	/** delete a compact record from a page */
	MLOG_COMP_REC_DELETE = 42

	/** delete compact record list end on index page */
	MLOG_COMP_LIST_END_DELETE = 43

	/*** delete compact record list start on index page */
	MLOG_COMP_LIST_START_DELETE = 44

	/** copy compact record list end to a new created index page */
	MLOG_COMP_LIST_END_COPY_CREATED = 45

	/** reorganize an index page */
	MLOG_COMP_PAGE_REORGANIZE = 46

	/** log record about creating an .ibd file, with format */
	MLOG_FILE_CREATE2 = 47

	/** write the node pointer of a record on a compressed
	  non-leaf B-tree page */
	MLOG_ZIP_WRITE_NODE_PTR = 48

	/** write the BLOB pointer of an externally stored column
	  on a compressed page */
	MLOG_ZIP_WRITE_BLOB_PTR = 49

	/** write to compressed page header */
	MLOG_ZIP_WRITE_HEADER = 50

	/** compress an index page */
	MLOG_ZIP_PAGE_COMPRESS = 51

	/** compress an index page without logging it's image */
	MLOG_ZIP_PAGE_COMPRESS_NO_DATA = 52

	/** reorganize a compressed page */
	MLOG_ZIP_PAGE_REORGANIZE = 53

	/** rename a tablespace file that starts with (space_id,page_no) */
	MLOG_FILE_RENAME2 = 54

	/** note the first use of a tablespace file since checkpoint */
	MLOG_FILE_NAME = 55

	/** note that all buffered log was written since a checkpoint */
	MLOG_CHECKPOINT = 56

	/** Create a R-Tree index page */
	MLOG_PAGE_CREATE_RTREE = 57

	/** create a R-tree compact page */
	MLOG_COMP_PAGE_CREATE_RTREE = 58

	/** this means that a file page is taken into use.
	  We use it to replace MLOG_INIT_FILE_PAGE. */
	MLOG_INIT_FILE_PAGE2 = 59

	/** Table is being truncated. (Marked only for file-per-table) */
	MLOG_TRUNCATE = 60 // Disabled for WL6378 */

	/** notify that an index tree is being loaded without writing
	  redo log about individual pages */
	MLOG_INDEX_LOAD = 61

	/** log for some persistent dynamic metadata change */
	MLOG_TABLE_DYNAMIC_META = 62

	/** create a SDI index page */
	MLOG_PAGE_CREATE_SDI = 63

	/** create a SDI compact page */
	MLOG_COMP_PAGE_CREATE_SDI = 64

	/** Used in tests of redo log. It must never be used outside unit tests. */
	MLOG_TEST = 65

	/** biggest value (used in assertions) */
	MLOG_BIGGEST_TYPE = MLOG_TEST
)

// Define the Undo record type.
// Reference mysql-5.7.19/storage/innobase/include/trx0rec.h
const (
	// fresh insert into clustered index
	TRX_UNDO_INSERT_REC = 11

	// update of a non-delete-marked record
	TRX_UNDO_UPD_EXIST_REC = 12

	// update of a delete marked record to a not delete marked record;
	// also the fields of the record can change
	TRX_UNDO_UPD_DEL_REC = 13

	// delete marking of a record; fields do not change
	TRX_UNDO_DEL_MARK_REC = 14

	// compilation info is multiplied by this and ORed to the type above
	TRX_UNDO_CMPL_INFO_MULT = 16

	// This bit can be ORed to type_cmpl to denote that we updated external storage fields:
	// used by purge to free the external storage
	TRX_UNDO_UPD_EXTERN = 128
)
