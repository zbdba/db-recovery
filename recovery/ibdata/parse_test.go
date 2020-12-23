package ibdata

import (
	"path/filepath"
	"runtime"
	"testing"
)

var p *Parse
var devPath string

func init() {
	p = NewParse()
	_, filename, _, _ := runtime.Caller(0)
	devPath = filepath.Dir(filepath.Dir(filepath.Dir(filename)))
}

func TestParseFile(t *testing.T) {
	// test parse ibdata
	pages, err := p.parseFile(devPath + "/cmd/test/fixture/ibdata1")
	if err != nil {
		t.Error(err.Error())
	}

	if len(pages) == 0 {
		t.Error("no pages found, parse error!")
	}

	// test parse ibd
	pages, err = p.parseFile(devPath + "/cmd/test/fixture/test/test_int.ibd")
	if err != nil {
		t.Error(err.Error())
	}

	if len(pages) == 0 {
		t.Error("no pages found, parse error!")
	}
}

func TestInsertColumns(t *testing.T) {
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
