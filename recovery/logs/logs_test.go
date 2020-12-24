package logs

import (
	"flag"
	"path/filepath"
	"runtime"
	"testing"
)

var devPath string
var fixturePath string
var mysqlRelease = flag.String("mysql-release", "mysql", "mysql docker image release vendor, eg. mysql, percona, mariadb")
var mysqlVersion = flag.String("mysql-version", "5.7", "mysql docker image versions, eg. 5.7, 8.0")

func init() {
	_, filename, _, _ := runtime.Caller(0)
	devPath = filepath.Dir(filepath.Dir(filepath.Dir(filename)))
	fixturePath = devPath + "/cmd/test/fixture/" + *mysqlRelease + "_" + *mysqlVersion
	InitLogs(devPath+"/cmd/test/fixture/", "debug")
}

func TestDebug(t *testing.T) {
	Debug("debug")
}

func TestError(t *testing.T) {
	Error("error")
}

func TestInfo(t *testing.T) {
	Info("info")
}

func TestWarn(t *testing.T) {
	Warn("warn")
}

func TestTrace(t *testing.T) {
	Trace("trace")
}

func TestFlushLogs(t *testing.T) {
	FlushLogs()
}
