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

// The default MySQL page size, size can be set 4k or 8k etc,
// if not set use to the default page size.
const DefaultPageSize int = 16384

// mysql-5.7.19/storage/innobase/include/fil0fil.h
// #define FIL_PAGE_INDEX		17855	/*!< B-tree node */
const FilPageIndex uint64 = 17855

// The root page number of the data dictionary.
// Use it can find the sys tables index root page.
const (
	SysTablesIdx  uint64 = 1
	SysColumnsIdx uint64 = 2
	SysIndexesIdx uint64 = 3
	SysFieldsIdx  uint64 = 4
	SystemPageIdx uint64 = 7
)
