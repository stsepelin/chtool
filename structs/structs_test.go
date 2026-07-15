package structs

import (
	"strings"
	"testing"
	"time"
)

type row struct {
	ID      int64     `ch:"id"`
	Name    string    `ch:"name"`
	Money   string    `ch:"money" chtype:"Decimal(14, 6)"`
	Tags    []string  `ch:"tags"`
	When    time.Time `ch:"created_at"`
	Skipped string    // no ch tag
	Ignored string    `ch:"-"`
}

func TestColumns(t *testing.T) {
	cols := Columns[row]()
	var names []string
	for _, c := range cols {
		names = append(names, c.Name)
	}
	if got := strings.Join(names, ","); got != "id,name,money,tags,created_at" {
		t.Fatalf("Columns = %q", got)
	}
}

func TestCreateDDL(t *testing.T) {
	ddl, err := CreateDDL[row]("events", "MergeTree", "id")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"`id` Int64", "`name` String", "`money` Decimal(14, 6)",
		"`tags` Array(String)", "`created_at` DateTime",
		"ENGINE = MergeTree", "ORDER BY id",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL missing %q:\n%s", want, ddl)
		}
	}
}

func TestCreateDDLUnmappedTypeErrors(t *testing.T) {
	type bad struct {
		Ch chan int `ch:"c"`
	}
	if _, err := CreateDDL[bad]("t", "MergeTree", "c"); err == nil {
		t.Fatal("expected error for an unmapped Go type without a chtype tag")
	}
}
