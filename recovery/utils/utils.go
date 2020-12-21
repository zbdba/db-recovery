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

import (
	`bytes`
	`encoding/binary`
	`encoding/hex`
	`fmt`
	`github.com/zbdba/db-recovery/recovery/utils/logs`
	`io/ioutil`
	`math`
	`os`
	`os/exec`
	`strconv`
	`strings`
	`time`
)

// Reference MySQL rec_get_status method
func RecGetStatus(rec []byte, offset uint64) uint64 {
	// #define REC_NEW_STATUS		3
	// #define REC_NEW_STATUS_MASK	0x7UL
	// #define REC_NEW_STATUS_SHIFT	0
	return RecGetBitField1(rec, offset, 3, 0x7, 0)
}

// Reference MySQL ut_align_offset method
func UtAlignOffset(rec []byte, AlignNo uint64) uint64 {
	return uint64(rec[0]) & (AlignNo - 1)
}

func GetIntValue(FixLength int, value []byte) interface{} {
	switch FixLength {
	case 1:
		v1 := (MatchReadFrom1(value) & 0x7F) | ((^MatchReadFrom1(value)) & 0x80)
		TinyInt := int(v1)
		TinyInt = TinyInt % TinyIntRange
		if TinyInt > (TinyIntRange/2-1) {
			TinyInt = TinyInt - TinyIntRange
		}
		return TinyInt
	case 2:
		v2 := (MatchReadFrom2(value) & 0x7FFF) | ((^MatchReadFrom2(value)) & 0x8000)
		SmallInt := int(v2)
		SmallInt = SmallInt % SmallIntRange
		if SmallInt > (SmallIntRange/2-1) {
			SmallInt = SmallInt - SmallIntRange
		}
		return SmallInt
	case 3:
		// TODO: comment
		v3 := MatchReadFrom3(value)
		if (v3 >> 23) == 1 {
			// Value is positive
			v3 &= 0x007FFFFF
			return v3
		} else {
			// Value is negative
			return int64(v3) ^ (-1 << (3 * 8 - 1))
		}

	case 4:
		v4 := (MatchReadFrom4(value) & 0x7FFFFFFF) | ((^MatchReadFrom4(value)) & 0x80000000)
		IntValue := int(v4)
		IntValue = IntValue % IntRange
		if IntValue > (IntRange/2-1) {
			IntValue = IntValue - IntRange
		}
		return IntValue
	case 8:
		v8 := (MatchReadFrom8(value) & 0x7FFFFFFFFFFFFFFF) |
			((^(MatchReadFrom8(value))) & 0x8000000000000000)
		return int64(v8)
	}

	return 0
}

func GetUintValue(FixLength int, value []byte) uint64 {
	switch FixLength {
	case 1:
		return MatchReadFrom1(value)
	case 2:
		return MatchReadFrom2(value)
	case 3:
		return MatchReadFrom3(value) & 0x3FFFFF
	case 4:
		return MatchReadFrom4(value)
	case 5:
		return MatchReadFrom8(value) >> 24
	case 6:
		return MatchReadFrom8(value) >> 16
	case 7:
		return MatchReadFrom8(value) >> 8
	case 8:
		return MatchReadFrom8(value)
	}
	return 0
}

// Reference MySQL rec_offs_nth_size method.
func RecOffsNthSize(offsets *[]uint64, n int) uint64 {
	if n == 0 {
		// REC_OFFS_MASK
		return (*offsets)[2:][1+n] & ((1 << 30) - 1)
	} else {
		// REC_OFFS_MASK
		return ((*offsets)[2:][1+n] - (*offsets)[2:][n]) & ((1 << 30) - 1)
	}
}

// Reference MySQL rec_offs_size method.
func RecOffsSize(offsets *[]uint64) uint64 {
	return RecOffsDataSize(offsets) + RecOffsExtraSize(offsets)
}

// Reference MySQL rec_offs_extra_size method.
func RecOffsExtraSize(offsets *[]uint64) uint64 {
	return (((*offsets)[2:])[0]) &^ ((1 << 31) | (1 << 30))
	//return ^((1 << 31) | (1 << 30))
}

// Reference MySQL rec_offs_data_size method.
func RecOffsDataSize(offsets *[]uint64) uint64 {
	return (*offsets)[2:][RecOffNFields(offsets)]
}

// Reference MySQL rec_offs_n_fields method.
func RecOffNFields(offsets *[]uint64) uint64 {
	return (*offsets)[1]
}

// Reference MySQL rec_1_get_field_end_info method.
func Rec1GetFieldEndInfo(d []byte, offset uint64, n uint64) uint64 {
	// #define REC_N_OLD_EXTRA_BYTES	6
	return MatchReadFrom1(d[offset-(6+n+1):])
}

// Reference MySQL rec_2_get_field_end_info method.
func Rec2GetFieldEndInfo(d []byte, offset uint64, n uint64) uint64 {
	// return(mach_read_from_2(rec - (REC_N_OLD_EXTRA_BYTES + 2 * n + 2)));
	return MatchReadFrom2(d[offset-(6+2*n+2):])
}

// Reference MySQL rec_get_1byte_offs_flag
func RecGet1byteOffsFlag(rec []byte, offset uint64) uint64 {
	// return (rec_get_bit_field_1(rec, REC_OLD_SHORT, REC_OLD_SHORT_MASK,
	// REC_OLD_SHORT_SHIFT));
	return RecGetBitField1(rec, offset, 3, 0x1, 0)
}

func RecGetBitField1(rec []byte, offset uint64, offs uint64, mask uint64, shift uint64) uint64 {
	//return (MatchReadFrom1(rec[(uint64(len(rec)) - offs):]) & mask) >> shift
	return (MatchReadFrom1(rec[(offset - offs):]) & mask) >> shift
}

//#define rec_get_nth_field(rec, offsets, n, len) \
//((rec) + rec_get_nth_field_offs(offsets, n, len))
func RecGetNthField(rec []byte, offsets []uint64, n int, length *uint64) []byte {
	offs := RecGetNthFieldOffs(offsets, n, length)
	return rec[offs:]
}

func RecGetNthFieldOffs(offsets []uint64, n int, len *uint64) uint64 {
	var offs uint64
	var length uint64
	if n == 0 {
		offs = 0
	} else {
		// #define rec_offs_base(offsets) (offsets + REC_OFFS_HEADER_SIZE)
		// #define REC_OFFS_MASK		(REC_OFFS_EXTERNAL - 1)
		// #define REC_OFFS_EXTERNAL	((ulint) 1 << 30)
		offs = (offsets[2:][n]) & ((1 << 30) - 1)
	}

	length = offsets[2:][n+1]

	// #define REC_OFFS_SQL_NULL	((ulint) 1 << 31)
	if (length & (1 << 31)) == 0 {
		length &= (1 << 30) - 1
		length -= offs
	} else {
		length = 0xFFFFFFFF
	}

	*len = length

	return offs
}

func ReadNextBytes(file *os.File, number int) ([]byte, error) {
	bytes := make([]byte, number)

	_, err := file.Read(bytes)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func MatchReadFrom1(b []byte) uint64 {
	return (uint64)(b[0])
}
func MatchReadFrom2(b []byte) uint64 {
	return ((uint64)(b[0]) << 8) | (uint64)(b[1])
}

func MatchReadFrom3(b []byte) uint64 {
	return ((uint64)(b[0]) << 16) |
		((uint64)(b[1]) << 8) |
		(uint64)(b[2])
}

func MatchReadFrom4(b []byte) uint64 {
	return ((uint64)(b[0]) << 24) |
		((uint64)(b[1]) << 16) |
		((uint64)(b[2]) << 8) |
		(uint64)(b[3])
}

func MatchReadFrom7(b []byte) uint64 {
	return UtUllCreate(MatchReadFrom3(b), MatchReadFrom4(b[3:]))
}

func UtUllCreate(high uint64, low uint64) uint64 {
	return high<<32 | low
}

func MatchReadFrom8(b []byte) uint64 {
	ull := MatchReadFrom4(b) << 32
	ull |= MatchReadFrom4(b[4:])
	return ull
}

func PageHeaderGetField(b []byte, field uint64) uint64 {
	return MatchReadFrom2(b[38+field:])
}

func PageIsComp(b []byte) uint64 {
	return PageHeaderGetField(b, 4) & 0x8000
}

func MatchParseCompressed(data []byte, pos uint64) (uint64, uint64, error) {

	Num1, err := strconv.ParseInt("80", 16, 64)
	Num2, err := strconv.ParseInt("C0", 16, 64)
	Num3, err := strconv.ParseInt("E0", 16, 64)
	Num4, err := strconv.ParseInt("F0", 16, 64)

	Num5, err := strconv.ParseInt("7FFF", 16, 64)
	Num6, err := strconv.ParseInt("3FFFFF", 16, 64)
	Num7, err := strconv.ParseInt("1FFFFFFF", 16, 64)

	if err != nil {
		logs.Error("convert data error, the error is ", err, " method is MatchParseCompressed")
		return 0, 0, err
	}

	flag := MatchReadFrom1(data[pos:])

	if int64(flag) < Num1 {
		return flag, 1, nil
	} else if int64(flag) < Num2 {
		return MatchReadFrom2(data[pos:]) & uint64(Num5), 2, nil
	} else if int64(flag) < Num3 {
		return MatchReadFrom3(data[pos:]) & uint64(Num6), 3, nil
	} else if int64(flag) < Num4 {
		return MatchReadFrom4(data[pos:]) & uint64(Num7), 4, nil
	} else {
		if pos+5 > uint64(len(data)) {
			ErrMsg := fmt.Sprintf("data too short, method is MatchParseCompressed")
			logs.Error(ErrMsg)
			return 0, 5, fmt.Errorf(ErrMsg)
		}
		return MatchReadFrom4(data[pos+1:]), 5, nil
	}
}

func MatchGetCompressedSize(n uint64) (uint64, error) {

	Num1, err := strconv.ParseInt("80", 16, 64)
	Num2, err := strconv.ParseInt("4000", 16, 64)
	Num3, err := strconv.ParseInt("200000", 16, 64)
	Num4, err := strconv.ParseInt("10000000", 16, 64)

	if err != nil {
		logs.Error("convert data error, the error is ", err, " method is MatchGetCompressedSize")
		return 0, err
	}

	if n < uint64(Num1) {
		return 1, nil
	} else if n < uint64(Num2) {
		return 2, nil
	} else if n < uint64(Num3) {
		return 3, nil
	} else if n < uint64(Num4) {
		return 4, nil
	} else {
		return 5, nil
	}

}

func ParseBinaryInt8(data []byte) int8 {
	return int8(data[0])
}
func ParseBinaryUint8(data []byte) uint8 {
	return data[0]
}
func ParseBinaryInt16(data []byte) int16 {
	return int16(binary.LittleEndian.Uint16(data))
}
func ParseBinaryUint16(data []byte) uint16 {
	return binary.LittleEndian.Uint16(data)
}

func ParseBinaryInt24(data []byte) int32 {
	u32 := uint32(ParseBinaryUint24(data))
	if u32&0x00800000 != 0 {
		u32 |= 0xFF000000
	}
	return int32(u32)
}
func ParseBinaryUint24(data []byte) uint32 {
	return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
}

func ParseBinaryInt32(data []byte) int32 {
	return int32(binary.LittleEndian.Uint32(data))
}
func ParseBinaryUint32(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

func ParseBinaryInt64(data []byte) int64 {
	return int64(binary.LittleEndian.Uint64(data))
}
func ParseBinaryUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

func ParseBinaryFloat32(data []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(data))
}

func ParseBinaryFloat64(data []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(data))
}

func MatchUllReadComPressed(b []byte, pos *uint64) uint64 {
	// TODO: num should add to pos
	n, num, err := MatchParseCompressed(b, *pos)
	*pos += num
	if err != nil {
		fmt.Println(err.Error())
	}
	size, err := MatchGetCompressedSize(n)
	// *pos += size
	if err != nil {
		fmt.Println(err.Error())
	}
	n <<= 32
	n |= MatchReadFrom4(b[*pos:][size:])
	*pos += 4
	return n
}

func MatchUllReadMuchCompressed(b []byte) (uint64, error) {
	var n uint64
	var size uint64
	Num1, err := strconv.ParseInt("FF", 16, 64)
	if err != nil {
		logs.Error("convert data to int error, the error is ", err)
		return 0, err
	}
	if b[0] != byte(Num1) {
		n = 0
		size = 0
	} else {
		n, _, err := MatchParseCompressed(b, 1)
		if err != nil {
			return 0, err
		}
		v, err := MatchGetCompressedSize(n)
		if err != nil {
			fmt.Println(err.Error())
		}
		size = 1 + v
	}
	value, _, err := MatchParseCompressed(b, size)
	if err != nil {
		return 0, err
	}

	n |= value

	return n, nil
}

func MachUllGetMuchCompressedSize(num uint64) uint64 {

	n, err := MatchGetCompressedSize(num)
	if err != nil {
		return 0
	}
	if (num >> 32) == 0 {
		return n
	}
	n1, err := MatchGetCompressedSize(num >> 32)

	Num, err := strconv.ParseInt("FFFFFFFF", 16, 64)
	n2, err := MatchGetCompressedSize(num & uint64(Num))
	if err != nil {
		fmt.Println(err.Error())
	}
	return n1 + n2
}

func ReadIntoStruct(file *os.File, dest interface{}, size int) error {
	data, err := ReadNextBytes(file, int(size))
	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer(data)
	err = binary.Read(buffer, binary.BigEndian, dest)
	if err != nil {
		//return errors.Wrap(err, "binary.Read failed")
		logs.Error("binary.Read file failed, the error is ", err)
		return err
	}
	return nil
}

func ParseIndex(comp bool, data []byte, pos uint64) (error, uint64) {
	if comp {
		if uint64(len(data)) < (pos + 4) {
			ErrMsg := fmt.Sprintf("data is to short")
			logs.Debug(ErrMsg)
			err := fmt.Errorf(ErrMsg)
			return err, 0
		}
		n := MatchReadFrom2(data[pos:])
		pos += 2
		uniq := MatchReadFrom2(data[pos:])
		pos += 2

		logs.Debug("the index have columns: ", n, "uniq index have columns: ", uniq)
		if uint64(len(data)) < (pos + n*2) {
			ErrMsg := fmt.Sprintf("data is to short")
			logs.Debug(ErrMsg)
			err := fmt.Errorf(ErrMsg)
			return err, 0
		}

		var i uint64
		for i = 0; i < n; i++ {
			DataLen := MatchReadFrom2(data[pos:])
			//fmt.Println("index data is ", string(data[pos+len:]))
			pos += 2
			logs.Debug("index column:", i, " len is ", DataLen)
		}
	}

	return nil, pos
}

func ParseInsertRecord(IsShort bool, data []byte, pos uint64) (error, uint64) {
	if !IsShort {
		offset := MatchReadFrom2(data[pos:])
		pos += 2
		logs.Debug("offset is ", offset)
	}

	EndSegLen, num, err := MatchParseCompressed(data, pos)
	if err != nil {
		return err, 0
	}

	pos += num

	if pos > uint64(len(data)) {
		ErrMsg := fmt.Sprintf("data is too short")
		logs.Debug(ErrMsg)
		err := fmt.Errorf(ErrMsg)
		return err, 0
	}

	Num1, err := strconv.ParseInt("1", 16, 64)

	if (EndSegLen & uint64(Num1)) != 0 {
		InfoAndStatusBits := MatchReadFrom1(data[pos:])
		pos += 1
		OriginOffset, num, err := MatchParseCompressed(data, pos)

		if err != nil {
			fmt.Println(err.Error())
		}
		pos += num

		MisMatchIndex, num, err := MatchParseCompressed(data, pos)

		if err != nil {
			fmt.Println(err.Error())
		}
		pos += num
		logs.Debug("InfoAndStatusBits is ", InfoAndStatusBits, "OriginOffset is ",
			OriginOffset, "MisMatchIndex is ", MisMatchIndex)
	}

	EndSegLen >>= 1
	logs.Debug("EndSegLen is ", EndSegLen)
	pos += EndSegLen

	return nil, pos
}

// TODO: deal with chinese, many character, should provide solutions.
func ParseData(DataType uint64, MySQLType uint64,
	data []byte, FieldLen uint64, FixLength int,
	IsUnsigned bool, IsBinary *bool) (interface{}, error) {

	switch DataType {
	case DATA_VARCHAR:
		return string(data[:FieldLen]), nil
	case DATA_CHAR:
		return string(data[:FieldLen]), nil
	case DATA_FIXBINARY:
		switch MySQLType {
		// TODO: deal with version 2.
		case MYSQL_TYPE_TIME:
			FormatTime := ParseTime(data)
			return FormatTime, nil
		case MYSQL_TYPE_TIMESTAMP:
			FormatTimeStamp := ParseTimeStamp(data)
			return FormatTimeStamp, nil
		case MYSQL_TYPE_DATETIME:
			FormatDateTime := ParseDateTime(data)
			return FormatDateTime, nil
		case MYSQL_TYPE_BIT:
			return GetUintValue(FixLength, data[:FieldLen]), nil
		case MYSQL_TYPE_NEWDECIMAL:
			// TODO: support it.
			fmt.Println("test MYSQL_TYPE_NEWDECIMAL")
		case MYSQL_TYPE_STRING:
			*IsBinary = true
			return ParseBlob(data[:FieldLen]), nil
		}
		return string(data[:FieldLen]), nil
	case DATA_BINARY:
		switch MySQLType {
			case MYSQL_TYPE_VARCHAR:
				*IsBinary = true
				return ParseBlob(data[:FieldLen]), nil
		default:
			if FixLength != 0 {
				return GetUintValue(FixLength, data[:FieldLen]), nil
			}
			return string(data[:FieldLen]), nil
		}
	case DATA_INT:
		switch MySQLType {
		case MYSQL_TYPE_DATE:
			DateTime := ParseDate(data)
			return DateTime, nil
		case MYSQL_TYPE_YEAR:
			FormatYear := MatchReadFrom1(data) + 1900
			return FormatYear, nil
		default:
			if IsUnsigned {
				return GetUintValue(FixLength, data[:FieldLen]), nil
			} else {
				return GetIntValue(FixLength, data[:FieldLen]), nil
			}
		}
	case DATA_FLOAT:
		FormatFloat := ParseFloat(data)
		return FormatFloat, nil
	case DATA_DOUBLE:
		FormatDouble := ParseDouble(data)
		return FormatDouble, nil

	case DATA_DECIMAL:
		// TODO: support
		logs.Info("the decimal data type, not support.")
	case DATA_VARMYSQL:
		return string(data[:FieldLen]), nil
	case DATA_MYSQL:
		return strings.TrimSpace(string(data[:FieldLen])), nil
	case DATA_BLOB:
		// TODO: deal with binary data.
		if *IsBinary {
			FormatBlobToHex := ParseBlob(data[:FieldLen])
			return FormatBlobToHex, nil
		} else {
			// TODO: when use chinese, client and server charset should be the same.
			return string(data[:FieldLen]), nil
		}
	}
	return nil, nil
}

func GetFixedLengthByMySQLType(MySQLType uint64, FieldLen uint64) uint64 {

	switch MySQLType {
	//case MYSQL_TYPE_DECIMAL:
	//case MYSQL_TYPE_TINY:
	//case MYSQL_TYPE_SHORT:
	case MYSQL_TYPE_LONG:

	//case MYSQL_TYPE_FLOAT:
	//case MYSQL_TYPE_DOUBLE:
	//case MYSQL_TYPE_NULL:
	//case MYSQL_TYPE_TIMESTAMP:
	//case MYSQL_TYPE_LONGLONG:
	//case MYSQL_TYPE_INT24:
	//case MYSQL_TYPE_DATE:
	//case MYSQL_TYPE_TIME:
	case MYSQL_TYPE_DATETIME:
		return 0
	//case MYSQL_TYPE_YEAR:
	//case MYSQL_TYPE_NEWDATE:
	case MYSQL_TYPE_VARCHAR:
		return 0
	//case MYSQL_TYPE_BIT:
	//case MYSQL_TYPE_TIMESTAMP2:
	//case MYSQL_TYPE_DATETIME2:
	//case MYSQL_TYPE_TIME2:
	//case MYSQL_TYPE_NEWDECIMAL:
	//case MYSQL_TYPE_ENUM:
	//case MYSQL_TYPE_SET:
	case MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB:
		return 0
	//case MYSQL_TYPE_VAR_STRING:
	//case MYSQL_TYPE_STRING:
	//case MYSQL_TYPE_GEOMETRY:
	default:
		return FieldLen
	}

	return 0
}

func GetFixedLength(DataType uint64, FieldLen uint64) uint64 {

	switch DataType {
	//case DATA_VARCHAR:
	//	return 0
	//case DATA_CHAR:
	//return FieldLen / 3
	//case DATA_FIXBINARY:
	//case DATA_BINARY:
	case DATA_BLOB:
		return 0
	//case DATA_INT:
	//	//return GetUintValue(FixLength, data), nil
	//case DATA_SYS_CHILD:
	//case DATA_SYS:
	//case DATA_FLOAT:
	//case DATA_DOUBLE:
	//case DATA_DECIMAL:
	case DATA_VARMYSQL:
		return 0
	//return FieldLen / 3
	case DATA_MYSQL:
		// return FieldLen / 3
		// char type should be return 0.
		return 0
	case DATA_BINARY:
		return FieldLen
	//case DATA_MTYPE_MAX:
	default:
		return FieldLen
	}
}

func GetMaxLength(MySQLType uint64) {
	// TODO: impl
	switch MySQLType {
	}
}

func GetTimeFormat(data []byte) int {
	d := MatchReadFrom8(data)
	var year, month, day, hour, min, sec int

	r := d &^ (1 << 63)
	sec = int(r % 100)
	r /= 100
	min = int(r % 100)
	r /= 100
	hour = int(r % 100)
	r /= 100
	day = int(r % 100)
	r /= 100
	month = int(r % 100)
	r /= 100
	year = int(d % 10000)
	if year > 1990 && year < 2100 && month >= 1 && month <= 12 &&
		day >= 1 && day <= 31 && hour >= 0 && hour <= 23 &&
		min >= 0 && min <= 59 && sec >= 0 && sec <= 59 {
		return 1
	}

	var sign uint64
	sign = 0
	sign = d >> 63
	if sign == 0 {
		return 0
	}

	yd := (d & 0x7FFFC00000000000) >> 46
	year = int(yd / 13)
	month = int(int(yd) - (year * 13))
	day = int((d & 0x00003E0000000000) >> 41)
	hour = int((d & 0x000001F000000000) >> 36)
	min = int((d & 0x0000000FC0000000) >> 30)
	sec = int((d & 0x000000003F000000) >> 24)
	if year > 1990 && year < 2100 && month >= 1 &&
		month <= 12 && day >= 1 && day <= 31 &&
		hour >= 0 && hour <= 23 && min >= 0 &&
		min <= 59 && sec >= 0 && sec <= 59 {
		return 2
	} else {
		return 0
	}
}

func ParseDateTime(data []byte) string {
	format := GetTimeFormat(data)
	if format == 1 || format == 0 {
		ldate := MatchReadFrom8(data)
		ldate = ldate &^ (1 << 63)
		sec := ldate % 100
		ldate /= 100
		min := ldate % 100
		ldate /= 100
		hour := ldate % 100
		ldate /= 100
		day := ldate % 100
		ldate /= 100
		month := ldate % 100
		ldate /= 100
		year := ldate % 10000

		FormatTime := fmt.Sprintf("%02d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, sec)

		return FormatTime

	} else if format == 2 {
		ldate := MatchReadFrom8(data)
		yd := (ldate & 0x7FFFC00000000000) >> 46
		year := yd / 13
		month := yd - year*13
		day := (ldate & 0x00003E0000000000) >> 41
		hour := (ldate & 0x000001F000000000) >> 36
		min := (ldate & 0x0000000FC0000000) >> 30
		sec := (ldate & 0x000000003F000000) >> 24

		FormatTime := fmt.Sprintf("%02d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, sec)

		return FormatTime
	}

	return ""

}

func ParseDate(data []byte) string {

	ldate := MatchReadFrom3(data)
	var year, month, day int
	ldate = ldate &^ (1 << 23)
	day = int(ldate % 32)
	ldate /= 32
	month = int(ldate % 16)
	ldate /= 16
	year = int(ldate)
	DateTime := fmt.Sprintf("%02d-%02d-%02d", year, month, day)
	return DateTime
}

func ParseTime(data []byte) string {

	// TODO: add time_precision and fixed_length.
	var hour, min, sec uint
	var IsNagative bool

	ltime := int64(MatchReadFrom3(data))
	ltime = ltime - 0x800000
	if ltime < 0 {
		IsNagative = true
		ltime = -ltime
	}

	// TODO: add param or remove this code.
	if (MySQLVersion == 5.6) {
		hour = uint((ltime & 0x3FF000) >> 12)
		min = uint((ltime & 0xFC0) >> 6)
		sec = uint(ltime & 0x3F)

		FormatTime := fmt.Sprintf("%02d:%02d:%02d", hour, min, sec)
		if IsNagative {
			FormatTime = "-" + FormatTime
		}
		return FormatTime
	} else {
		ltime = ltime &^ (1 << 23)
		sec = uint(int(ltime % 60))
		ltime /= 60
		min = uint(int(ltime % 60))
		ltime /= 60
		hour = uint(int(ltime % 24))
		FormatTime := fmt.Sprintf("%02d:%02d:%02d", hour, min, sec)

		return FormatTime
	}
}

func ParseTimeStamp(data []byte) string {
	// TODO: add time_precision
	t := MatchReadFrom4(data)
	tm := time.Unix(int64(t), 0).Format("2006-01-02 03:04:05")
	return tm
}

func ParseFloat(data []byte) float32 {
	bits := binary.LittleEndian.Uint32(data[:4])
	float := math.Float32frombits(bits)
	return float
}

func ParseDouble(data []byte) float64 {
	bits := binary.LittleEndian.Uint64(data[:8])
	double := math.Float64frombits(bits)
	return double
}

func ParseBlob(data []byte) string {
	dst := make([]byte, hex.EncodedLen(len(data)))
	hex.Encode(dst, data)
	FormatDst := fmt.Sprintf("%s", dst)
	return FormatDst
}

func EscapeValue(colValue string) string {
	// TODO: confirm other conditions will make error or inconsistent.
	var esc string
	colBuffer := *new(bytes.Buffer)
	last := 0
	for i, c := range colValue {
		switch c {
		case 0:
			esc = `\0`
		case '\n':
			esc = `\n`
		case '\r':
			esc = `\r`
		case '\\':
			esc = `\\`
		case '\'':
			esc = `\'`
		case '"':
			esc = `\"`
		case '\032':
			esc = `\Z`
		default:
			continue
		}
		colBuffer.WriteString(colValue[last:i])
		colBuffer.WriteString(esc)
		last = i + 1
	}
	colBuffer.WriteString(colValue[last:])
	// TODO: If the source is really 0001-01-01T00:00:00Z, it will cause data inconsistency.
	if colBuffer.String() == "0001-01-01T00:00:00Z" {
		colBuffer.Reset()
		colBuffer.WriteString("")
	}
	return colBuffer.String()
}

func GetFilesFromOS(FilePath string) ([]string, error) {
	c := fmt.Sprintf("ls -lrt %s|grep ibd|awk '{print $9}'|awk -F '.' '{print $1}'", FilePath)
	cmd := exec.Command("/bin/bash", "-c", c)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Printf("Error:can not obtain stdout pipe for command:%s\n", err)
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		fmt.Println("Error:The command is err,", err.Error())
		return nil, err
	}

	bytes, err := ioutil.ReadAll(stdout)
	if err != nil {
		fmt.Println("ReadAll Stdout:", err.Error())
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		fmt.Println("wait:", err.Error())
		return nil, err
	}
	fmt.Println(strings.Split(string(bytes), "\n"))
	return strings.Split(string(bytes), "\n"), nil
}

// FixedLengthInt: little endian
func FixedLengthInt(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(i) * 8)
	}
	return num
}
