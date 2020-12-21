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

package utils

// The MySQL InnoDB data type
const (
	// The 'MAIN TYPE' of a column
	// missing column
	DATA_MISSING uint64 = iota

	// character varying of the latin1_swedish_ci charset-collation;
	// note that the MySQL format for this, DATA_BINARY, DATA_VARMYSQL,
	// is also affected by whether the 'precise type' contains DATA_MYSQL_TRUE_VARCHAR
	DATA_VARCHAR

	// fixed length character of the latin1_swedish_ci charset-collation
	DATA_CHAR

	// binary string of fixed length
	DATA_FIXBINARY

	// binary string
	DATA_BINARY

	// binary large object, or a TEXT type; if prtype & DATA_BINARY_TYPE == 0,
	// then this is actually a TEXT column (or a BLOB created with < 4.0.14;
	// since column prefix indexes came only in 4.0.14,
	// the missing flag in BLOBs created before that does not cause any harm)
	DATA_BLOB

	// integer: can be any size 1 - 8 bytes
	DATA_INT

	// address of the child page in node pointer
	DATA_SYS_CHILD

	// system column Data types >= DATA_FLOAT
	// must be compared using the whole field, not as binary strings
	DATA_SYS
	DATA_FLOAT
	DATA_DOUBLE

	// decimal number stored as an ASCII string
	DATA_DECIMAL

	// any charset varying length char
	DATA_VARMYSQL

	// any charset fixed length char NOTE that 4.1.1 used DATA_MYSQL and
	// DATA_VARMYSQL for all character sets, and the charset-collation for
	// tables created with it can also be latin1_swedish_ci
	DATA_MYSQL

	// dtype_store_for_order_and_null_size()
	DATA_MTYPE_MAX
)

// The MySQL Server type
const (
	MYSQL_TYPE_DECIMAL uint64 = iota
	MYSQL_TYPE_TINY
	MYSQL_TYPE_SHORT
	MYSQL_TYPE_LONG
	MYSQL_TYPE_FLOAT
	MYSQL_TYPE_DOUBLE
	MYSQL_TYPE_NULL
	MYSQL_TYPE_TIMESTAMP
	MYSQL_TYPE_LONGLONG
	MYSQL_TYPE_INT24
	MYSQL_TYPE_DATE
	MYSQL_TYPE_TIME
	MYSQL_TYPE_DATETIME
	MYSQL_TYPE_YEAR
	MYSQL_TYPE_NEWDATE
	MYSQL_TYPE_VARCHAR
	MYSQL_TYPE_BIT
	MYSQL_TYPE_TIMESTAMP2
	MYSQL_TYPE_DATETIME2
	MYSQL_TYPE_TIME2
	MYSQL_TYPE_NEWDECIMAL  = 246
	MYSQL_TYPE_ENUM        = 247
	MYSQL_TYPE_SET         = 248
	MYSQL_TYPE_TINY_BLOB   = 249
	MYSQL_TYPE_MEDIUM_BLOB = 250
	MYSQL_TYPE_LONG_BLOB   = 251
	MYSQL_TYPE_BLOB        = 252
	MYSQL_TYPE_VAR_STRING  = 253
	MYSQL_TYPE_STRING      = 254
	MYSQL_TYPE_GEOMETRY    = 255
)

const (
	MySQLVersion = 5.6
)

// The MySQL integer data type range.
const (
	TinyIntRange = 128 * 2
	SmallIntRange = 32768 * 2
	IntRange = 2147483648 * 2
)