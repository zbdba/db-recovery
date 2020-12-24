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

package logs

import (
	"flag"
	"strconv"
	"strings"

	"github.com/golang/glog"
)

const (
	LevelFatal = iota
	LevelError
	LevelWarn
	LevelInfo
	LevelDebug
	LevelTrace
)

var LevelMap = map[string]int{
	"trace": LevelTrace,
	"debug": LevelDebug,
	"info":  LevelInfo,
	"warn":  LevelWarn,
	"error": LevelError,
	"fatal": LevelFatal,
}

func InitLogs(path, LogLevel string) error {
	err := flag.Set("log_dir", path)
	if err != nil {
		return err
	}
	level, ok := LevelMap[strings.ToLower(LogLevel)]
	if !ok {
		level = LevelInfo
	}
	err = flag.Set("v", strconv.Itoa(level))
	return err
}

func FlushLogs() {
	glog.Flush()
}

func Trace(args ...interface{}) {
	if glog.V(glog.Level(LevelTrace)) {
		glog.Infoln(args...)
	}
}

func Debug(args ...interface{}) {
	if glog.V(glog.Level(LevelDebug)) {
		glog.Infoln(args...)
	}
}

func Info(args ...interface{}) {
	if glog.V(glog.Level(LevelInfo)) {
		glog.Infoln(args...)
	}
}

func Warn(args ...interface{}) {
	if glog.V(glog.Level(LevelWarn)) {
		glog.Warningln(args...)
	}
}

func Error(args ...interface{}) {
	if glog.V(glog.Level(LevelError)) {
		glog.Errorln(args...)
	}
}

func Fatal(args ...interface{}) {
	if glog.V(glog.Level(LevelFatal)) {
		glog.Fatalln(args...)
	}
}
