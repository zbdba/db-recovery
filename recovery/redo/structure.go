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

// Checkpoint the MySQL Innodb redo file checkpoint info.
type Checkpoint struct {
	// 0x00 8 Log checkpoint number
	Number uint64

	// 0x08 8 Log sequence number of checkpoint
	LSN uint64

	// 0x10 4 Offset to the log entry, calculated by log_group_calc_lsn_offset() [19]
	Offset uint32

	// 0x14 4 Size of the buffer (a fixed value: 2 · 1024 · 1024)
	BufferSize uint32

	// 0x18 8 Archived log sequence number. If UNIV_LOG_ARCHIVE is not activated,
	// InnoDB inserts FF FF FF FF FF FF FF FF here.
	ArchivedLSN uint64

	// 0x20 256 Spacing and padding
	_ [256]byte

	// 0x120 4 Checksum 1 (validating the contents from offset 0x00 to 0x19F)
	Checksum1 uint32

	// 0x124 4 Checksum 2 (validating the block without the log sequence number,
	// but including checksum 1, i.e. values from 0x08 to0x124)
	Checksum2 uint32

	// 0x128 4 Current fsp free limit in tablespace 0, given in units of one megabyte;
	// used by ibbackup to decide if unused ends of non-auto-extending data files in
	// space 0 can be truncated [20]
	CurrentFSP uint32

	// 0x12C 4 Magic number that tells if the checkpoint contains the field
	// above (added to InnoDB version 3.23.50 [20])
	Magic uint32

	// Padding
	_ [208]byte
}

// LogBlock the MySQL Innodb redo block
type LogBlock struct {
	// 0x00 Log block header number. If the most significant bit is 1,
	// the following block is the first block in a log flush write segment. [20].
	HeaderNumber uint32

	// Number of bytes written to this block.
	BlockSize uint16

	// Offset to the first start of a log record group of
	// this block (see II-D3 for further details).
	Offset uint16

	// Number of the currently active checkpoint (see II-C).
	CurrentActiveCheckpoint uint32

	//HdrSize                 uint16 // Hdr-size
	Data [496]byte

	//  Checksum of the log block contents. In InnoDB versions 3.23.52
	//  or earlier this did not contain the checksum but the same value
	//  as LOG_BLOCK_HDR_NO [20].  Table IV
	Checksum uint32
}
