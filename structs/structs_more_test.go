package structs

import (
	"strings"
	"testing"
)

type scalars struct {
	B   bool    `ch:"b"`
	I8  int8    `ch:"i8"`
	I16 int16   `ch:"i16"`
	I32 int32   `ch:"i32"`
	I64 int64   `ch:"i64"`
	I   int     `ch:"i"`
	U8  uint8   `ch:"u8"`
	U16 uint16  `ch:"u16"`
	U32 uint32  `ch:"u32"`
	U64 uint64  `ch:"u64"`
	U   uint    `ch:"u"`
	F32 float32 `ch:"f32"`
	F64 float64 `ch:"f64"`
	S   string  `ch:"s"`
}

func TestCreateDDLScalarTypeMapping(t *testing.T) {
	ddl, err := CreateDDL[scalars]("t", "MergeTree", "i64")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"`b` Bool", "`i8` Int8", "`i16` Int16", "`i32` Int32", "`i64` Int64", "`i` Int64",
		"`u8` UInt8", "`u16` UInt16", "`u32` UInt32", "`u64` UInt64", "`u` UInt64",
		"`f32` Float32", "`f64` Float64", "`s` String",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL missing %q:\n%s", want, ddl)
		}
	}
}

// A pointer to a struct should be dereferenced by both Columns and CreateDDL.
func TestPointerStructIsDereferenced(t *testing.T) {
	if cols := Columns[*scalars](); len(cols) != 14 {
		t.Fatalf("expected 14 columns from *scalars, got %d", len(cols))
	}
	if _, err := CreateDDL[*scalars]("t", "MergeTree", "i64"); err != nil {
		t.Fatalf("CreateDDL[*scalars]: %v", err)
	}
}

func TestCreateDDLNoTaggedFieldsErrors(t *testing.T) {
	type none struct {
		A string
		B int `ch:"-"`
	}
	if _, err := CreateDDL[none]("t", "MergeTree", "x"); err == nil {
		t.Fatal("expected an error when no ch:-tagged fields exist")
	}
}

func TestColumnGoTypeAndName(t *testing.T) {
	cols := Columns[scalars]()
	if cols[0].Name != "b" || cols[0].GoType != "bool" || cols[0].Field != "B" {
		t.Fatalf("unexpected first column: %+v", cols[0])
	}
}

func TestDiffString(t *testing.T) {
	d := Diff{Column: "views", Issue: "in struct but missing from table"}
	if got := d.String(); got != "views: in struct but missing from table" {
		t.Fatalf("Diff.String = %q", got)
	}
}

func TestCreateDDLSliceOfUnmappedTypeErrors(t *testing.T) {
	type bad struct {
		Chans []chan int `ch:"chans"`
	}
	if _, err := CreateDDL[bad]("t", "MergeTree", "chans"); err == nil {
		t.Fatal("expected an error for an Array of an unmapped element type")
	}
}

func TestCreateDDLNestedSliceType(t *testing.T) {
	type nested struct {
		Xs [][]int64 `ch:"xs"`
	}
	ddl, err := CreateDDL[nested]("t", "MergeTree", "xs")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(ddl, "`xs` Array(Array(Int64))") {
		t.Fatalf("nested slice not mapped: %s", ddl)
	}
}
