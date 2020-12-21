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
	BlockSize = 512
)

// The MySQL Innodb redo record type.
const (
	MLOG_1BYTE uint64 = iota + 1
	MLOG_2BYTES
	_
	MLOG_4BYTES
	_
	_
	_
	MLOG_8BYTES
	// record insert
	MLOG_REC_INSERT

	// mark clustered index record deleted
	MLOG_REC_CLUST_DELETE_MARK

	// mark secondary index record deleted
	MLOG_REC_SEC_DELETE_MARK

	_
	// update of a record, record field sizes
	MLOG_REC_UPDATE_IN_PLACE

	// delete a record from a page
	MLOG_REC_DELETE

	// delete record list end on index page
	MLOG_LIST_END_DELETE

	// delete record list start on index page
	MLOG_LIST_START_DELETE

	// copy record list end to a new created index page
	MLOG_LIST_END_COPY_CREATED

	// reorganize an index page in ROW_FORMAT = REDUNDANT
	MLOG_PAGE_REORGANIZE

	// create an index page
	MLOG_PAGE_CREATE

	// insert entry in an undo log
	MLOG_UNDO_INSERT

	// erase an undo log page end
	MLOG_UNDO_ERASE_END

	// initialize a page in an undo log
	MLOG_UNDO_INIT

	// discard an update undo log header
	MLOG_UNDO_HDR_DISCARD

	// reuse an insert undo log header
	MLOG_UNDO_HDR_REUSE

	// create an undo log header
	MLOG_UNDO_HDR_CREATE

	// mark an index record as the predefined minimum record
	MLOG_REC_MIN_MARK

	// initialize an ibuf bitmap page
	MLOG_IBUF_BITMAP_INIT

	// full contents of a page
	// MLOG_FULL_PAGE
	_
	// current LSN
	// MLOG_LSN

	// this means that a file page is taken into use and
	// the prior contents of the page should be ignored:
	// in recovery we must not trust the lsn values stored to the file page
	MLOG_INIT_FILE_PAGE

	// write a string to a page
	MLOG_WRITE_STRING

	// if a single mtr writes several log records,
	// this log record ends the sequence of these records
	MLOG_MULTI_REC_END

	// dummy log record used to pad a log block full
	MLOG_DUMMY_RECORD

	// log record about an .ibd file creation
	MLOG_FILE_CREATE

	// log record about an .ibd file rename
	MLOG_FILE_RENAME

	// log record about an .ibd file deletion
	MLOG_FILE_DELETE

	// mark a compact index record as the predefined minimum record
	MLOG_COMP_REC_MIN_MARK

	// create a compact index page
	MLOG_COMP_PAGE_CREATE

	// compact record insert
	MLOG_COMP_REC_INSERT

	// mark compact clustered index record deleted
	MLOG_COMP_REC_CLUST_DELETE_MARK

	// mark compact secondary index record deleted; this log record type is
	// redundant, as MLOG_REC_SEC_DELETE_MARK is independent of the record format.
	MLOG_COMP_REC_SEC_DELETE_MARK

	// update of a compact record, preserves record field sizes
	MLOG_COMP_REC_UPDATE_IN_PLACE

	// delete a compact record from a page
	MLOG_COMP_REC_DELETE

	// delete compact record list end on index page
	MLOG_COMP_LIST_END_DELETE

	// delete compact record list start on index page
	MLOG_COMP_LIST_START_DELETE

	// copy compact record list end to a new created index page
	MLOG_COMP_LIST_END_COPY_CREATED

	// reorganize an index page
	MLOG_COMP_PAGE_REORGANIZE

	// log record about creating an.ibd file, with format
	MLOG_FILE_CREATE2

	// write the node pointer of a record on a compressed non-leaf B-tree page
	MLOG_ZIP_WRITE_NODE_PTR

	// write the BLOB pointer of an externally stored column on a compressed page
	MLOG_ZIP_WRITE_BLOB_PTR

	// write to compressed page header
	MLOG_ZIP_WRITE_HEADER

	// compress an index page
	MLOG_ZIP_PAGE_COMPRESS

	// compress an index page without logging it's image
	MLOG_ZIP_PAGE_COMPRESS_NO_DATA

	// reorganize a compressed page
	MLOG_ZIP_PAGE_REORGANIZE

	// add by mysql-5.7.19

	// rename a tablespace file that starts with (space_id,page_no)
	MLOG_FILE_RENAME2

	// note the first use of a tablespace file since checkpoint
	MLOG_FILE_NAME

	// note that all buffered log was written since a checkpoint
	MLOG_CHECKPOINT

	// Create a R-Tree index page
	MLOG_PAGE_CREATE_RTREE

	// create a R-tree compact page
	MLOG_COMP_PAGE_CREATE_RTREE

	// this means that a file page is taken into use. We use it to replace MLOG_INIT_FILE_PAGE
	MLOG_INIT_FILE_PAGE2

	// Table is being truncated. (Marked only for file-per-table)
	MLOG_TRUNCATE

	// notify that an index tree is being loaded without writing redo log about individual pages
	MLOG_INDEX_LOAD
	//  biggest value (used in assertions )
	//MLOG_BIGGEST_TYPE
)

// Define the Undo record type.
// Reference mysql-5.7.19/storage/innobase/include/trx0rec.h
const (
	// fresh insert into clustered index
	TRX_UNDO_INSERT_REC    = 11

	// update of a non-delete-marked record
	TRX_UNDO_UPD_EXIST_REC = 12

	// update of a delete marked record to a not delete marked record;
	// also the fields of the record can change
	TRX_UNDO_UPD_DEL_REC   = 13

	// delete marking of a record; fields do not change
	TRX_UNDO_DEL_MARK_REC   = 14

	// compilation info is multiplied by this and ORed to the type above
	TRX_UNDO_CMPL_INFO_MULT = 16

	// This bit can be ORed to type_cmpl to denote that we updated external storage fields:
	// used by purge to free the external storage
	TRX_UNDO_UPD_EXTERN     = 128
)
