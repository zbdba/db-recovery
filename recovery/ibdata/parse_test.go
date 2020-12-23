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
	pages, err := p.ParseFile(devPath + "/cmd/test/fixture/ibdata1")
	if err != nil {
		t.Error(err.Error())
	}

	if len(pages) == 0 {
		t.Error("no pages found, parse error!")
	}

	// test parse ibd
	pages, err = p.ParseFile(devPath + "/cmd/test/fixture/test/test_int.ibd")
	if err != nil {
		t.Error(err.Error())
	}

	if len(pages) == 0 {
		t.Error("no pages found, parse error!")
	}
}
