package ibdata

import (
	"flag"
	"path/filepath"
	"runtime"
	"testing"
)

var p *Parse
var devPath string
var fixturePath string
var mysqlRelease = flag.String("mysql-release", "mysql", "mysql docker image release vendor, eg. mysql, percona, mariadb")
var mysqlVersion = flag.String("mysql-version", "5.7", "mysql docker image versions, eg. 5.7, 8.0")

func init() {
	p = NewParse()
	_, filename, _, _ := runtime.Caller(0)
	devPath = filepath.Dir(filepath.Dir(filepath.Dir(filename)))
	fixturePath = devPath + "/cmd/test/fixture/" + *mysqlRelease + "_" + *mysqlVersion
}

func TestParse(t *testing.T) {
	err := p.ParseDictPage(fixturePath + "/ibdata1")
	if err != nil {
		t.Error(err.Error())
	}

	err = p.ParseDataPage(fixturePath+"/ibdata1", "test", "test_int", true)
	if err != nil {
		t.Error(err.Error())
	}
}

func TestParseFile(t *testing.T) {
	// test parse ibdata
	pages, err := p.parseFile(fixturePath + "/ibdata1")
	if err != nil {
		t.Error(err.Error())
	}

	if len(pages) == 0 {
		t.Error("no pages found, parse error!")
	}

	// test parse ibd
	pages, err = p.parseFile(fixturePath + "/test/test_int.ibd")
	if err != nil {
		t.Error(err.Error())
	}

	if len(pages) == 0 {
		t.Error("no pages found, parse error!")
	}
}

func TestAddColumns(t *testing.T) {
	var columns []Columns
	// add first
	columns = addColumns(columns, 0, Columns{FieldName: "first"})

	// append last
	columns = addColumns(columns, 1, Columns{FieldName: "last"})

	// add into middle
	columns = addColumns(columns, 1, Columns{FieldName: "middle"})

	if len(columns) != 3 {
		t.Error("addColumns count error")
	}

	if columns[0].FieldName != "first" {
		t.Error("addColumns value error")
	}

	if columns[1].FieldName != "middle" {
		t.Error("addColumns value error")
	}

	if columns[2].FieldName != "last" {
		t.Error("addColumns value error")
	}
}
