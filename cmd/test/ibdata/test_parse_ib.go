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

	"github.com/zbdba/db-recovery/recovery/ibdata"
	"github.com/zbdba/db-recovery/recovery/logs"
)

func main() {

	// init logger
	flag.Parse()

	InitErr := logs.InitLogs("/data/db-recovery/logs", "DEBUG")
	if InitErr != nil {
		fmt.Println(InitErr)
	}

	p := ibdata.NewParse()

	//err := p.ParseDictPage("/root/online_data_test/2020-01-02-15/ibdata1")
	//err := p.ParseDictPage("/root/recovery/ibdata1")
	err := p.ParseDictPage("/data/mysql3322/data/ibdata1")
	if err != nil {
		fmt.Println(err)
	}

	//AllFiles, GetFileErr := utils.GetFilesFromOS("/root/online_data_test/2020-01-02-15/ks3report")
	//
	//if GetFileErr != nil {
	//	return
	//}
	//
	//for _, file := range AllFiles {
	//	if file == "" {
	//		continue
	//	}
	//
	//	fmt.Println("start parse table: ", file)
	//	FilePath := fmt.Sprintf("/root/online_data_test/2020-01-02-15/ks3report/%s.ibd", file)
	//	RecoveryErr := p.RecoveryDeleteData(FilePath,
	//		"ks3report", file)
	//	if RecoveryErr != nil {
	//		fmt.Println(RecoveryErr)
	//	}
	//}

	RecoveryErr := p.ParseDataPage("/data/mysql3322/data/type_test/test_int.ibd",
		"type_test", "test_int", false)
	//RecoveryErr = p.ParseDataPage("/data/mysql3322/data/type_test/test_float.ibd",
	//	"type_test", "test_float", false)
	//RecoveryErr = p.ParseDataPage("/data/mysql3322/data/type_test/test_date.ibd",
	//	"type_test", "test_date", false)
	//RecoveryErr = p.ParseDataPage("/data/mysql3322/data/type_test/test_string.ibd",
	//	"type_test", "test_string", false)
	//RecoveryErr := p.ParseDataPage("/root/recovery/xuser_newsession.ibd",
	//	"userSession", "xuser_newsession", true)
	//RecoveryErr := p.RecoveryDeleteData("/root/online_data_test/2020-01-02-15/ks3report/user_data_report.ibd",
	//	"ks3report", "user_data_report")
	if RecoveryErr != nil {
		fmt.Println(RecoveryErr)
	}

	//
	//f := func(k, v interface{}) bool {
	//
	//	fmt.Println(k, v)
	//	t, ok := v.(ibdata.Tables)
	//	//for _, f := range t.Fields {
	//	//	fmt.Println(f.FieldName, f.FieldType, f.FieldPos, f.FieldLen)
	//	//}
	//	if ok {
	//		for _, idx := range t.Indexes {
	//			f1 := func(k, v interface{}) bool {
	//
	//				fmt.Println(k, v)
	//
	//				return true
	//			}
	//			idx.Range(f1)
	//		}
	//	}
	//
	//	return true
	//}
	//
	//p.M.Range(f)

}
