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

package main

import (
	"flag"
	"fmt"

	"github.com/zbdba/db-recovery/recovery/logs"
	"github.com/zbdba/db-recovery/recovery/redo"
)

func main() {

	// init logger
	flag.Parse()

	InitErr := logs.InitLogs("/data/github.com/zbdba/db-recovery", "DEBUG")
	if InitErr != nil {
		fmt.Println(InitErr)
	}

	p, err := redo.NewParse("/data/mysql3322/data/ibdata1", "", "")
	//p, err := redo.NewParse("/root/recovery/ibdata1")
	//p, err := redo.NewParse("/root/online_data_test/2020-01-16-23/ibdata1")

	if err != nil {
		fmt.Println(err)
	}

	//LogFileList := []string{"/data/mysql3308/data/ib_logfile0", "/data/mysql3308/data/ib_logfile1"}
	//LogFileList := []string{"/root/recovery/ib_logfile0"}
	//LogFileList := []string{"/root/recovery/ib_logfile0", "/root/recovery/ib_logfile1"}
	LogFileList := []string{"/data/mysql3322/data/ib_logfile0"}
	//LogFileList := []string{"/data/ib_logfile0"}
	ParseErr := p.Parse(LogFileList)

	//LogFileList := []string{"/root/online_data_test/2020-01-16-23/ib_logfile0"}
	//ParseErr := p.Parse(LogFileList)
	if ParseErr != nil {
		fmt.Println("ParseErr is ", ParseErr)
	}
}
