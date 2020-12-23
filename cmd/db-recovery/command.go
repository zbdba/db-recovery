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
	"runtime"
	"strings"

	"github.com/zbdba/db-recovery/recovery/ibdata"
	"github.com/zbdba/db-recovery/recovery/logs"
	"github.com/zbdba/db-recovery/recovery/redo"

	"github.com/spf13/cobra"
)

const (
	cliName        = "github.com/zbdba/db-recovery"
	cliDescription = "A simple command line client for github.com/zbdba/db-recovery."
)

var (
	SysDataFile string
	TableFile   string
	DBName      string
	TableName   string

	OpType string

	// redo info.
	RedoFile string

	// set log info.
	LogPath  string
	LogLevel string
)

func NewRootCommand(use, short string) *cobra.Command {
	rc := &cobra.Command{
		Use:        use,
		Short:      short,
		SuggestFor: []string{use},
	}
	rc.PersistentFlags().StringVar(&OpType, "OpType", "", "The OpType can be RecoveryData, RecoveryStruct, PrintData.")
	rc.PersistentFlags().StringVar(&LogPath, "LogPath", "/tmp", "set the log file path.")
	rc.PersistentFlags().StringVar(&LogLevel, "LogLevel", "DEBUG", "set the log level.")
	rc.AddCommand(NewRecoveryCommand())
	rc.AddCommand(NewVersionCommand())
	return rc
}

func NewRecoveryCommand() *cobra.Command {
	jc := &cobra.Command{
		Use:   "recovery <subcommand>",
		Short: "recovery related commands",
	}
	jc.AddCommand(NewFromDataFileCommand())
	jc.AddCommand(NewFromRedoFileCommand())
	return jc
}

func NewFromDataFileCommand() *cobra.Command {
	jc := &cobra.Command{
		Use:   "FromDataFile [option]",
		Short: "recovery from data file",
		Run:   FromDataFile,
	}
	jc.Flags().StringVar(&SysDataFile, "SysDataFile", "", "The path of system tablespace data file.")
	_ = jc.MarkFlagRequired("SysDataFile")

	jc.Flags().StringVar(&TableFile, "TableDataFile", "", "The path of Table tablespace file.")
	_ = jc.MarkFlagRequired("TableDataFile")

	jc.Flags().StringVar(&DBName, "DBName", "", "The database name.")
	_ = jc.MarkFlagRequired("DBName")

	jc.Flags().StringVar(&TableName, "TableName", "", "The table name.")
	_ = jc.MarkFlagRequired("TableName")

	return jc
}

func FromDataFile(cmd *cobra.Command, args []string) {

	// init logger
	flag.Parse()
	InitErr := logs.InitLogs(LogPath, LogLevel)
	if InitErr != nil {
		fmt.Println(InitErr.Error())
		return
	}

	IsRecovery := false

	p := ibdata.NewParse()
	err := p.ParseDictPage(SysDataFile)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if OpType == "RecoveryData" {
		IsRecovery = true
	}

	RecoveryErr := p.ParseTableData(TableFile, DBName, TableName, IsRecovery)
	if RecoveryErr != nil {
		fmt.Println(RecoveryErr)
	}

	// flush logs
	logs.FlushLogs()
}

func NewFromRedoFileCommand() *cobra.Command {
	jc := &cobra.Command{
		Use:   "FromRedoFile [option]",
		Short: "recovery from redo file",
		Run:   FromRedoFile,
	}
	jc.Flags().StringVar(&SysDataFile, "SysDataFile", "", "The path of system tablespace data file.")
	_ = jc.MarkFlagRequired("SysDataFile")

	jc.Flags().StringVar(&RedoFile, "RedoFile", "", "The path of redo log file, "+
		"it may have many redo log files, identify like: 'redo1','redo2'")
	_ = jc.MarkFlagRequired("RedoFile")

	jc.Flags().StringVar(&DBName, "DBName", "", "identify the database name which you want to recover.")

	jc.Flags().StringVar(&TableName, "TableName", "", "identify the table name which you want to recover.")

	return jc
}

func FromRedoFile(cmd *cobra.Command, args []string) {

	// init logger
	flag.Parse()
	InitErr := logs.InitLogs(LogPath, LogLevel)
	if InitErr != nil {
		fmt.Println(InitErr.Error())
		return
	}

	p, err := redo.NewParseRedo(SysDataFile, TableName, DBName)

	if err != nil {
		logs.Error("parse redo failed, the error is ", err.Error())
		return
	}

	LogFileList := strings.Split(RedoFile, ",")
	ParseErr := p.Parse(LogFileList)

	if ParseErr != nil {
		fmt.Println("parse redo failed, the error is ", ParseErr.Error())
	}

	// flush logs
	logs.FlushLogs()
}

func NewVersionCommand() *cobra.Command {
	vc := &cobra.Command{
		Use:   "version",
		Short: "Print version info",
		Run:   versionCommandFunc,
	}
	return vc
}

func versionCommandFunc(cmd *cobra.Command, args []string) {
	fmt.Println(logo())
	fmt.Printf("Project Name:%s\n", ProjectName)
	fmt.Printf("Version %d.%d.%d\n", Major, Minor, Patch)
	fmt.Printf("Git SHA: %s\n", GitSHA)
	fmt.Printf("Build Time:%s\n", BuildTime)
	fmt.Printf("Go Version:%s\n", runtime.Version())
	fmt.Printf("Go OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
}
