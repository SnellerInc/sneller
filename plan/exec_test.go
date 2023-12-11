// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package plan

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ints"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/vm"
)

// testenv is an Env that
// can read files from the
// ../testdata/ directory
type testenv struct {
	t       testing.TB
	tmp     string
	lock    sync.Mutex
	schema  expr.Hint
	indexer Indexer

	// Stat failure message, for testing
	// query planning errors
	mustfail string
}

func (t *testenv) fsys() *blockfmt.DirFS {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.tmp != "" {
		return blockfmt.NewDirFS(t.tmp)
	}
	t.tmp = t.t.TempDir()
	dir, err := filepath.Abs("../testdata")
	if err != nil {
		t.t.Fatal(err)
	}
	err = os.Symlink(dir, filepath.Join(t.tmp, "testdata"))
	if err != nil {
		t.t.Fatal(err)
	}
	return blockfmt.NewDirFS(t.tmp)
}

func (t *testenv) Schema(tbl expr.Node) expr.Hint {
	return t.schema
}

var _ Indexer = (*testenv)(nil)

func (t *testenv) Index(tbl expr.Node) (Index, error) {
	if t.indexer == nil {
		return nil, nil
	}
	return t.indexer.Index(tbl)
}

func (t *testenv) str2json(arg expr.Node) (*Input, error) {
	str, ok := arg.(expr.String)
	if !ok {
		return nil, fmt.Errorf("unexpected argument to NDJSON: %s", arg)
	}
	src := struct {
		*strings.Reader
		io.Closer
	}{strings.NewReader(string(str)), io.NopCloser(nil)}
	name := uuid() + ".zion"
	up, err := blockfmt.NewDirFS(t.tmp).Create(name)
	if err != nil {
		return nil, err
	}
	c := blockfmt.Converter{
		Inputs: []blockfmt.Input{{
			Size: src.Size(),
			R:    src,
			F:    blockfmt.MustSuffixToFormat(".json"),
		}},
		Output:     up,
		Comp:       "zion",
		Align:      1024,
		FlushMeta:  50 * 1024,
		TargetSize: 8 * 1024,
	}
	err = c.Run()
	if err != nil {
		return nil, err
	}
	tr := c.Trailer()
	blocks := ints.Intervals{{0, len(tr.Blocks)}}
	return &Input{
		Descs: []Descriptor{{
			Descriptor: blockfmt.Descriptor{
				ObjectInfo: blockfmt.ObjectInfo{
					Path: filepath.Join(name),
				},
				Trailer: *tr,
			},
			Blocks: blocks,
		}},
	}, nil
}

func (t *testenv) Stat(tbl expr.Node, h *Hints) (*Input, error) {
	b, ok := tbl.(*expr.Builtin)
	if ok {
		switch b.Name() {
		case "JSON":
			return t.str2json(b.Args[0])
		default:
			return nil, fmt.Errorf("don't understand builtin %s", expr.ToString(tbl))
		}
	}
	str, ok := tbl.(expr.Ident)
	if !ok {
		return nil, fmt.Errorf("don't understand table expression %s", expr.ToString(tbl))
	}
	if t.mustfail != "" {
		return nil, errors.New(t.mustfail)
	}
	path := "testdata/" + string(str) + ".zion"
	f, err := t.fsys().Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	tr, err := blockfmt.ReadTrailer(f.(io.ReaderAt), info.Size())
	if err != nil {
		return nil, fmt.Errorf("reading trailer: %v", err)
	}
	blocks := ints.Intervals{{0, len(tr.Blocks)}}
	return &Input{
		Descs: []Descriptor{{
			Descriptor: blockfmt.Descriptor{
				ObjectInfo: blockfmt.ObjectInfo{Path: path},
				Trailer:    *tr,
			},
			Blocks: blocks,
		}},
	}, nil
}

// Run implements Runner.Run
func (t *testenv) Run(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	if t.mustfail != "" {
		return errors.New(t.mustfail)
	}
	r := FSRunner{FS: t.fsys()}
	return r.Run(dst, src, ep)
}

var _ TableLister = (*testenv)(nil)

func (t *testenv) ListTables(db string) ([]string, error) {
	if db != "" {
		return nil, fmt.Errorf("no such database: %s", db)
	}
	ds, err := os.ReadDir(filepath.Join(t.tmp, "testdata"))
	if err != nil {
		return nil, err
	}
	var list []string
	for i := range ds {
		name := ds[i].Name()
		if !strings.HasSuffix(name, ".zion") {
			continue
		}
		list = append(list, strings.TrimSuffix(name, ".zion"))
	}
	return list, nil
}

func countmsg(n int) string {
	return fmt.Sprintf(`{"count": %d}`, n)
}

func rowcount(t *testing.T, buf []byte) int {
	count := 0
	for len(buf) > 0 {
		if ion.IsBVM(buf) {
			buf = buf[4:]
			continue
		}
		if ion.TypeOf(buf) == ion.StructType {
			count++
		}
		skip := ion.SizeOf(buf)
		if skip > len(buf) {
			t.Errorf("row %d: can't skip %d bytes (have %d)...", count, skip, len(buf))
			return count
		}
		buf = buf[skip:]
	}
	return count
}

// partial produces a partial schema
// from pairs of identifiers and type sets, i.e.
//
//	partial("x", expr.UnsignedType, ...)
//
// any references that are not part of the
// schema are returned as AnyType rather than
// MissingType as they would be for a complete schema
type partialHint struct {
	field string
	ts    expr.TypeSet
}

func (h *partialHint) TypeOf(e expr.Node) expr.TypeSet {
	if p, ok := e.(expr.Ident); ok {
		if string(p) == h.field {
			return h.ts
		}
	}

	return expr.AnyType
}

func partial(field string, ts expr.TypeSet) expr.Hint {
	return &partialHint{
		field: field,
		ts:    ts,
	}
}

const (
	nycTaxiBytes = 1048576
	parkingBytes = 1048576
)

func toJSON(st *ion.Symtab, d ion.Datum) string {
	if d.IsEmpty() {
		return "<nil>"
	}
	var ib ion.Buffer
	ib.StartChunk(st)
	d.Encode(&ib, st)
	br := bufio.NewReader(bytes.NewReader(ib.Bytes()))
	var sb strings.Builder
	_, err := ion.ToJSON(&sb, br)
	if err != nil {
		panic(err)
	}
	return sb.String()
}

func TestExec(t *testing.T) {
	env := &testenv{t: t}

	tcs := []struct {
		// schema, if non-nil, is the
		// schema used for all input tables
		schema expr.Hint
		// indexer, if non-nil, is used to
		// produce indexes during planning
		indexer Indexer
		// query is the literal query text
		query string
		// rows is the number of expected rows;
		// use this if you leave expectedRows unset
		rows int
		// first row is the expected contents of
		// the first row, or "" if expectedRows is used
		firstrow string
		// expectedRows is the JSON representation
		// of the expected output (in order!);
		expectedRows []string
		// matchPlan is a set of regular expressions
		// that are supposed to match the textual query plan
		matchPlan []string
		// expectBytes, if non-zero, is the number
		// of bytes we expect the query to scan
		expectBytes int
	}{
		{
			query: `SELECT COUNT(*), VendorID FROM nyc_taxi GROUP BY VendorID ORDER BY SUM(trip_distance) DESC`,
			expectedRows: []string{
				`{"count": 7353, "VendorID": "VTS"}`,
				`{"count": 1055, "VendorID": "CMT"}`,
				`{"count":  152, "VendorID": "DDS"}`,
			},
		},
		{
			query:       `select * from nyc_taxi`,
			rows:        8560,
			expectBytes: nycTaxiBytes,
		},
		{
			query:       `select COUNT(*) from nyc_taxi t`,
			rows:        1,
			firstrow:    countmsg(8560),
			expectBytes: nycTaxiBytes,
		},
		{
			query:       `select COUNT(*) from parking where Make is missing`,
			rows:        1,
			firstrow:    countmsg(4),
			expectBytes: parkingBytes,
		},
		{
			// reverse of above:
			query:       `select COUNT(Make) from parking`,
			rows:        1,
			firstrow:    countmsg(1023 - 4),
			expectBytes: parkingBytes,
		},
		{
			query:       "select COUNT(*) from nyc_taxi where tpep_pickup_datetime<`2009-01-16T00:05:31Z`",
			rows:        1,
			firstrow:    countmsg(4057),
			expectBytes: nycTaxiBytes,
		},
		{
			query:       "select COUNT(*) from nyc_taxi where tpep_pickup_datetime>`2009-01-16T00:05:31Z`",
			rows:        1,
			firstrow:    countmsg(4502),
			expectBytes: nycTaxiBytes,
		},
		{
			query:       "select COUNT(*) from nyc_taxi where tpep_pickup_datetime>=`2009-01-16T00:05:31Z`",
			rows:        1,
			firstrow:    countmsg(4503),
			expectBytes: nycTaxiBytes,
		},
		{
			query:       "select COUNT(*) from nyc_taxi where tpep_pickup_datetime=`2009-01-16T00:05:31Z`",
			rows:        1,
			firstrow:    countmsg(1),
			expectBytes: nycTaxiBytes,
		},
		{
			query:       "select COUNT(*) from nyc_taxi where tpep_pickup_datetime between `2009-01-15T00:00:00Z` and `2009-01-15T23:59:59Z`",
			rows:        1,
			firstrow:    countmsg(350),
			expectBytes: nycTaxiBytes,
		},
		{
			// partiql oddity: MISSING is not NULL
			query:       `select COUNT(*) from parking where Make is not null`,
			rows:        1,
			firstrow:    countmsg(1023 - 4),
			expectBytes: parkingBytes,
		},
		{
			// test coalesce in projection position
			query: `select coalesce(Make, 'unknown') as mk from parking where Make is missing`,
			expectedRows: []string{
				`{"mk": "unknown"}`,
				`{"mk": "unknown"}`,
				`{"mk": "unknown"}`,
				`{"mk": "unknown"}`,
			},
			expectBytes: parkingBytes,
		},
		{
			// test CASE in projection position that isn't a COALESCE
			//
			// note: we need the nested query here to avoid the fact
			// that we don't have support for hashing boxed values yet
			// (i.e. we cannot compute DISTINCT of a boxed CASE)
			query: `
select distinct pronounce from
  (select
    (case
     when VendorID = 'VTS' then 'vee tee ess'
     when VendorID = 'CMT' then 'cee emm tee'
     when VendorID = 'DDS' then 'dee dee ess'
     else NULL
     end) as pronounce
   from nyc_taxi)
order by pronounce`,
			expectedRows: []string{
				`{"pronounce": "cee emm tee"}`,
				`{"pronounce": "dee dee ess"}`,
				`{"pronounce": "vee tee ess"}`,
			},
		},
		{
			// all tickets are greater than zero,
			// so tickets greater than zero or TRUE
			// should yield all rows
			query: `
select count(*)
from parking
where case
      when Make is not null then Ticket > 0
      else true
      end
`,
			rows:        1,
			firstrow:    countmsg(1023),
			expectBytes: parkingBytes,
		},
		{
			// there are 122 actual Make="HOND" entries,
			// so using "HOND" as the default Make value
			// should yield exactly 4 more
			query:       `select count(*) from parking where coalesce(Make, 'HOND') = 'HOND'`,
			rows:        1,
			firstrow:    countmsg(122 + 4),
			expectBytes: parkingBytes,
		},
		{
			query: `select coalesce(x, y, z) as val
from json('{"x": 1}{"y": 2}{"z": 3}')
where coalesce(x, y, z) > 2`,
			rows:     1,
			firstrow: `{"val": 3}`,
		},
		{
			query: `select row
from json('
  {"x": 1, "y": 2, "a": 3, "row": 0}
  {"y": 2, "z": 3, "row": 1}
  {"z": 3, "x": 4, "row": 2}
  {"z": 4, "y": 3, "x": 2, "row": 3}
  {"x": 4, "y": 3, "row": 4}
  {"y": 5, "a": 3, "row": 5}
  {"z": 6, "row": 6}
')
where coalesce(x, y, z) = 3`,
			rows: 0, // no rows match, because the first non-null coalesce arg is never 3
		},
		{
			query:    `select avg(fare_amount) from nyc_taxi`,
			rows:     1,
			firstrow: fmt.Sprintf(`{"avg": %g}`, 9.475478898890179),
		},
		{
			query: `select avg(fare_amount), VendorID from nyc_taxi group by VendorID order by avg(fare_amount)`,
			rows:  3,
			expectedRows: []string{
				`{"VendorID": "VTS", "avg": 9.435699641166867}`,
				`{"VendorID": "CMT", "avg": 9.685402771563982}`,
				`{"VendorID": "DDS", "avg": 9.942763085526314}`,
			},
		},
		// Test arithmetic expressions with immediate values, which should use optimized bytecode.
		{
			query:    `select MAX(Ticket) from parking`,
			rows:     1,
			firstrow: `{"max": 4272473892}`,
		},
		{
			query:    `select MIN(-Ticket) from parking`,
			rows:     1,
			firstrow: `{"min": -4272473892}`,
		},
		{
			query:    `select MAX(Ticket + 1) from parking`,
			rows:     1,
			firstrow: `{"max": 4272473893}`,
		},
		{
			query:    `select MAX(1 + Ticket) from parking`,
			rows:     1,
			firstrow: `{"max": 4272473893}`,
		},
		{
			query:    `select MAX(Ticket - 1) from parking`,
			rows:     1,
			firstrow: `{"max": 4272473891}`,
		},
		{
			query:    `select MAX(1 - Ticket) from parking`,
			rows:     1,
			firstrow: `{"max": -1103341115}`,
		},
		{
			query:    `select MAX(Ticket * 2) from parking`,
			rows:     1,
			firstrow: `{"max": 8544947784}`,
		},
		{
			query:    `select MAX(2 * Ticket) from parking`,
			rows:     1,
			firstrow: `{"max": 8544947784}`,
		},
		{
			query:    `select MAX(Ticket * 2 + 1) from parking`,
			rows:     1,
			firstrow: `{"max": 8544947785}`,
		},
		{
			query:    `select MAX(2 * Ticket + 1) from parking`,
			rows:     1,
			firstrow: `{"max": 8544947785}`,
		},
		{
			query:    `select MAX(1 + 2 * Ticket) from parking`,
			rows:     1,
			firstrow: `{"max": 8544947785}`,
		},
		{
			query:    `select MAX(Ticket * 2 - 1) from parking`,
			rows:     1,
			firstrow: `{"max": 8544947783}`,
		},
		{
			query:    `select MAX(Ticket / 2) from parking`,
			rows:     1,
			firstrow: `{"max": 2136236946}`,
		},
		{
			query:    `select MAX(Ticket / 2 + 1) from parking`,
			rows:     1,
			firstrow: `{"max": 2136236947}`,
		},
		{
			query:    `select MAX(1 + Ticket / 2) from parking`,
			rows:     1,
			firstrow: `{"max": 2136236947}`,
		},
		{
			query:    `select MAX(Ticket / 2 - 1) from parking`,
			rows:     1,
			firstrow: `{"max": 2136236945}`,
		},
		{
			query:    `select MAX(1 - Ticket / 2) from parking`,
			rows:     1,
			firstrow: `{"max": -551670557}`,
		},
		{
			query:    `select MAX(Ticket % 1000) from parking`,
			rows:     1,
			firstrow: `{"max": 996}`,
		},
		{
			query:    `select MAX(Ticket % 1000) from parking`,
			rows:     1,
			firstrow: `{"max": 996}`,
		},
		// Test arithmetic expressions with itself, special cases.
		{
			query:    `select MAX(PlateExpiry) from parking`,
			rows:     1,
			firstrow: `{"max": 201905}`,
		},
		{
			query:    `select MAX(PlateExpiry - PlateExpiry) from parking`,
			rows:     1,
			firstrow: `{"max": 0}`,
		},
		{
			query:    `select MAX(PlateExpiry + PlateExpiry) from parking`,
			rows:     1,
			firstrow: `{"max": 403810}`,
		},
		{
			query:    `select MAX(PlateExpiry * PlateExpiry) from parking`,
			rows:     1,
			firstrow: `{"max": 40765629025}`,
		},
		{
			query:    `select MAX(PlateExpiry / PlateExpiry) from parking`,
			rows:     1,
			firstrow: `{"max": 1}`,
		},
		// Test arithmetic functions.
		{
			query:    `select MAX(LEAST(PlateExpiry, IssueTime)) from parking`,
			rows:     1,
			firstrow: `{"max": 2355}`,
		},
		{
			query:    `select MAX(SQRT(PlateExpiry + 60239)) from parking`,
			rows:     1,
			firstrow: `{"max": 512}`,
		},
		{
			query:    `select MAX(ABS(PlateExpiry - 100000)) from parking`,
			rows:     1,
			firstrow: `{"max": 101905}`,
		},
		{
			query:    `select MAX(SIGN(Ticket)) from parking`,
			rows:     1,
			firstrow: `{"max": 1}`,
		},
		{
			query:    `select MAX(SIGN(-Ticket)) from parking`,
			rows:     1,
			firstrow: `{"max": -1}`,
		},
		// These are weird queries, SIGN(Ticket) always returns 1, so we use
		// some arithmetic to prepare a value that will be used with rounding.
		{
			query:    `select MAX(ROUND(SIGN(Ticket) - 0.5)) from parking`,
			rows:     1,
			firstrow: `{"max": 1}`,
		},
		{
			query:    `select MAX(ROUND_EVEN(SIGN(Ticket) - 0.5)) from parking`,
			rows:     1,
			firstrow: `{"max": 0}`,
		},
		{
			query:    `select MAX(TRUNC(SIGN(Ticket) + 0.5)) from parking`,
			rows:     1,
			firstrow: `{"max": 1}`,
		},
		{
			query:    `select MAX(TRUNC(SIGN(-Ticket) - 0.5)) from parking`,
			rows:     1,
			firstrow: `{"max": -1}`,
		},
		{
			query:    `select MAX(FLOOR(SIGN(Ticket) + 0.5)) from parking`,
			rows:     1,
			firstrow: `{"max": 1}`,
		},
		{
			query:    `select MAX(CEIL(SIGN(Ticket) + 0.5)) from parking`,
			rows:     1,
			firstrow: `{"max": 2}`,
		},
		// Missing support.
		{
			query:    `select Ticket from parking where Make is missing and Fine is missing`,
			rows:     1,
			firstrow: `{"Ticket": 1112092391}`,
		},
		{
			// test that a bare path expression in logical operator position
			// yields the equivalent of <expr> IS TRUE
			query:    `select count(*) from parking2 where Fields[0]`,
			rows:     1,
			firstrow: countmsg(882),
		},
		{
			query:    `select count(*) from parking2 where Fields[0] is false`,
			rows:     1,
			firstrow: countmsg(1023 - 882),
		},
		{
			query:    `select count(*) from parking2 where Color is null`,
			rows:     1,
			firstrow: countmsg(7),
		},
		{
			// select count(*)
			// from nyc_taxi t
			// where t.passenger_count>1 or t.trip_distance<1
			//
			// -> 4699
			query: `
select count(*)
from nyc_taxi t
where t.passenger_count>1 or t.trip_distance<1`,
			rows:     1,
			firstrow: countmsg(4699),
		},
		{
			query: `
select t.VendorID as vendor, t.fare_amount as fare, t.passenger_count as passengers
from nyc_taxi t
where t.passenger_count>1 or t.trip_distance<1`,
			rows:     4699,
			firstrow: `{"vendor": "VTS", "fare": 12.1, "passengers": 3}`,
		},
		{
			query: `
select out.Make as make, entry.Color as color
from parking3 as out, out.Entries as entry
where entry.Color = 'BK'
`,
			rows:     221,
			firstrow: `{"make": "ACUR", "color": "BK"}`,
		},
		{
			query: `
select out.Make as make, entry.Ticket as ticket, entry.Color as color
from parking3 as out, out.Entries as entry
where out.Make = 'CHRY' and entry.BodyStyle = 'PA'
`,
			rows:     12,
			firstrow: `{"make": "CHRY", "ticket": 1106506435, "color": "GO"}`,
		},
		{
			query:    `select min(passenger_count), sum(fare_amount) as sum from nyc_taxi`,
			rows:     1,
			firstrow: `{"min": 1, "sum": 81110.09937449993}`,
		},
		{
			query:    `select fare_amount + 0.1, total_amount + 0.5, total_amount - 1 from nyc_taxi limit 1`,
			rows:     1,
			firstrow: `{"_1": 9, "_2": 9.9, "_3": 8.4}`,
		},
		{
			query:    `select count(Make) from parking`,
			rows:     1,
			firstrow: countmsg(1019),
		},
		{
			// NOTE: this only works because parking.zion
			// is only one block; otherwise the output
			// is indeterminate...
			query:    `select Ticket as ticket from parking limit 1`,
			rows:     1,
			firstrow: `{"ticket": 1103341116}`,
		},
		{
			// see note above
			query:    `select Make as make from parking where Color = 'BK' AND BodyStyle = 'PA' limit 1`,
			rows:     1,
			firstrow: `{"make": "NISS"}`,
		},
		{
			query:    `select Make || ' - ' || BodyStyle from parking Where Color = 'BK' limit 1`,
			rows:     1,
			firstrow: `{"_1": "NISS - PA"}`,
		},
		{
			// find the most common Make for parking tickets
			query:    `select Make, COUNT(Make) as count from parking group by Make order by COUNT(Make) DESC limit 1`,
			rows:     1,
			firstrow: `{"Make": "HOND", "count": 122}`,
		},
		{
			// compute the same result as above using HAVING
			query: `select Make, count(Make) as c from parking group by Make having count(Make) = 122`,
			expectedRows: []string{
				`{"Make": "HOND", "c": 122}`,
			},
		},
		{
			// find the least common Make for parking tickets
			// (breaking the tie on Make ordering)
			query:    `select Make, COUNT(Make) as count from parking group by Make order by COUNT(Make), Make limit 1`,
			rows:     1,
			firstrow: `{"Make": "CHEC", "count": 1}`,
		},
		{
			// really round-about way of computing count(Ticket) where Make is not missing;
			// this exercises the SUM_INT() specialization for simple aggregates
			query:    `select sum(c) from (select count(Ticket) as c, Make from parking group by Make)`,
			rows:     1,
			firstrow: `{"sum": 1019}`,
			matchPlan: []string{
				"AGGREGATE.*SUM_INT",
			},
		},
		{
			// with the integer schema information,
			// this should yield a plan with SUM_INT()
			// during hash aggregation
			schema: partial("Fine", expr.IntegerType|expr.MissingType),
			query:  `select sum(Fine), Make from parking group by Make order by sum(Fine) desc, Make`,
			matchPlan: []string{
				"HASH AGGREGATE.*SUM_INT",
			},
			expectedRows: []string{
				`{"Make": "HOND", "sum": 8715}`,
				`{"Make": "TOYO", "sum": 7073}`,
				`{"Make": "FORD", "sum": 6863}`,
				`{"Make": "TOYT", "sum": 5700}`,
				`{"Make": "NISS", "sum": 5405}`,
				`{"Make": "CHEV", "sum": 5019}`,
				`{"Make": "BMW", "sum": 3035}`,
				`{"Make": "DODG", "sum": 2701}`,
				`{"Make": "VOLK", "sum": 2276}`,
				`{"Make": "HYUN", "sum": 2188}`,
				`{"Make": "JEEP", "sum": 1758}`,
				`{"Make": "KIA", "sum": 1628}`,
				`{"Make": "LEXU", "sum": 1434}`,
				`{"Make": "CHRY", "sum": 1309}`,
				`{"Make": "MERZ", "sum": 1287}`,
				`{"Make": "GMC", "sum": 1215}`,
				`{"Make": "MAZD", "sum": 1119}`,
				`{"Make": "INFI", "sum": 980}`,
				`{"Make": "ACUR", "sum": 954}`,
				`{"Make": "SUBA", "sum": 842}`,
				`{"Make": "MBNZ", "sum": 814}`,
				`{"Make": "AUDI", "sum": 788}`,
				`{"Make": "MITS", "sum": 724}`,
				`{"Make": "LINC", "sum": 656}`,
				`{"Make": "LEXS", "sum": 584}`,
				`{"Make": "PTRB", "sum": 454}`,
				`{"Make": "UNK", "sum": 454}`,
				`{"Make": "CADI", "sum": 453}`,
				`{"Make": "PONT", "sum": 422}`,
				`{"Make": "VOLV", "sum": 393}`,
				`{"Make": "FIAT", "sum": 345}`,
				`{"Make": "MNNI", "sum": 292}`,
				`{"Make": "BUIC", "sum": 277}`,
				`{"Make": "OTHR", "sum": 247}`,
				`{"Make": "BENZ", "sum": 214}`,
				`{"Make": "MERC", "sum": 199}`,
				`{"Make": "PORS", "sum": 194}`,
				`{"Make": "SATU", "sum": 191}`,
				`{"Make": "LROV", "sum": 175}`,
				`{"Make": "SUZU", "sum": 173}`,
				`{"Make": "FRHT", "sum": 171}`,
				`{"Make": "PLYM", "sum": 166}`,
				`{"Make": "OLDS", "sum": 161}`,
				`{"Make": "SCIO", "sum": 126}`,
				`{"Make": "STRN", "sum": 98}`,
				`{"Make": "HINO", "sum": 93}`,
				`{"Make": "ISU", "sum": 73}`,
				`{"Make": "JAGU", "sum": 73}`,
				`{"Make": "RROV", "sum": 73}`,
				`{"Make": "JAGR", "sum": 68}`,
				`{"Make": "CHEC", "sum": 63}`,
				`{"Make": "FREI", "sum": 63}`,
				`{"Make": "KW", "sum": 63}`,
				`{"Make": "LIND", "sum": 50}`,
				`{"Make": "MASE", "sum": 25}`,
				`{"Make": "SAA", "sum": 25}`,
				`{"Make": "SUZI", "sum": 25}`,
				`{"Make": "TESL", "sum": 25}`,
				`{"Make": "TSMR", "sum": 25}`,
			},
		},
		{
			// find the body style with the higest fine
			query:    `select BodyStyle, max(Fine) as fine from parking group by BodyStyle order by fine desc limit 1`,
			rows:     1,
			firstrow: `{"BodyStyle": "PA", "fine": 363}`,
		},
		{
			// there is one entry with Fine=NULL; ensure that
			// it doesn't pollute the output...
			query:    `select BodyStyle, min(Fine), max(Fine) from parking group by BodyStyle order by min(Fine)`,
			rows:     10,
			firstrow: `{"BodyStyle": "PA", "min": 25, "max": 363}`,
		},
		{
			// test projection of simple boolean expression
			query:    `select Make, Ticket = 1103341116 as yes from parking limit 1`,
			rows:     1,
			firstrow: `{"Make": "HOND", "yes": true}`,
		},
		{
			// the result of a matching CASE expression
			// that yields MISSING should still be MISSING,
			// even when there is an ELSE present
			query: `select
Make as make,
(case when Ticket = 1103341116 then dne else NULL end) as nope
from parking limit 1`,
			rows:     1,
			firstrow: `{"make": "HOND"}`,
		},
		{
			// test projection of expression yielding MISSING
			query:    `select Make, does_not_exist < 3 as dne from parking limit 1`,
			rows:     1,
			firstrow: `{"Make": "HOND"}`, // no 'dne column, since the expression yields MISSING'
		},
		{
			// test (TRUE AND MISSING) -> MISSING
			query:    `select Make, (Ticket = 1103341116 AND does_not_exist < 3) as e from parking limit 1`,
			rows:     1,
			firstrow: `{"Make": "HOND"}`,
		},
		{
			// test (FALSE AND MISSING) -> FALSE
			query:    `select Make, (Ticket <> 1103341116 AND does_not_exist < 3) as e from parking limit 1`,
			rows:     1,
			firstrow: `{"Make": "HOND", "e": false}`,
		},
		{
			// test (FALSE OR MISSING) -> MISSING
			query:    `select Make, (Ticket <> 1103341116 OR does_not_exist < 3) as e from parking limit 1`,
			rows:     1,
			firstrow: `{"Make": "HOND"}`,
		},
		{
			// test (FALSE OR MISSING) IS TRUE -> FALSE
			query:    `select Make, (Ticket <> 1103341116 OR does_not_exist < 3) IS TRUE as e from parking limit 1`,
			rows:     1,
			firstrow: `{"Make": "HOND", "e": false}`,
		},
		{
			// test (TRUE OR MISSING) IS FALSE -> FALSE
			query:    `select Make, (Ticket = 1103341116 OR does_not_exist = 3) IS FALSE as e from parking limit 1`,
			rows:     1,
			firstrow: `{"Make": "HOND", "e": false}`,
		},
		{
			// test (FALSE OR MISSING) IS FALSE -> FALSE
			query:    `select Make, (Ticket <> 1103341116 OR does_not_exist = 3) IS FALSE as e from parking limit 1`,
			rows:     1,
			firstrow: `{"Make": "HOND", "e": false}`,
		},
		{
			// test (FALSE AND MISSING) IS FALSE -> TRUE
			query:    `select Make, (Ticket <> 1103341116 AND does_not_exist = 3) IS FALSE as e from parking limit 1`,
			rows:     1,
			firstrow: `{"Make": "HOND", "e": true}`,
		},
		{
			// test (TRUE OR MISSING) -> TRUE
			query:    `select Make, (Ticket = 1103341116 OR does_not_exist < 3) as e from parking limit 1`,
			rows:     1,
			firstrow: `{"Make": "HOND", "e": true}`,
		},
		{
			// test emitting literals during projection
			query:    `select Ticket, 'hello' as greeting, 3 as an_int from parking where Make = 'JAGU'`,
			rows:     1,
			firstrow: `{"Ticket": 4271686823, "greeting": "hello", "an_int": 3}`,
		},
		{
			// test (logical_expr) = (logical_expr)
			query:    `select count(*) from parking where (Ticket = 1103341116) = (Make = 'HOND')`,
			rows:     1,
			firstrow: countmsg(898), // 897 + 1
		},
		{
			// test SELECT DISTINCT where every row is identical
			query:    `select distinct RatecodeID as id from nyc_taxi`,
			rows:     1,
			firstrow: `{"id": 0}`,
		},
		{
			// test SELECT DISTINCT where every row is unique
			query:    `select distinct Ticket from parking`,
			rows:     1023,
			firstrow: `{"Ticket": 1103341116}`,
		},
		{
			// test SELECT DISTINCT on column with known cardinality
			query: `select distinct Color from parking order by Color`,
			expectedRows: []string{
				`{"Color": "BG"}`, `{"Color": "BK"}`, `{"Color": "BL"}`, `{"Color": "BN"}`,
				`{"Color": "BR"}`, `{"Color": "BU"}`, `{"Color": "GN"}`, `{"Color": "GO"}`,
				`{"Color": "GR"}`, `{"Color": "GY"}`, `{"Color": "MA"}`, `{"Color": "MR"}`,
				`{"Color": "OR"}`, `{"Color": "OT"}`, `{"Color": "PR"}`, `{"Color": "RD"}`,
				`{"Color": "RE"}`, `{"Color": "SI"}`, `{"Color": "SL"}`, `{"Color": "TA"}`,
				`{"Color": "TN"}`, `{"Color": "WH"}`, `{"Color": "WT"}`, `{"Color": "YE"}`,
			},
		},
		{
			query:    `select count(distinct Color) from parking`,
			rows:     1,
			firstrow: `{"count": 24}`,
		},
		{
			// count the number of distinct colors occuring for each Make
			query:    `select count(distinct Color), Make from parking group by Make order by count(distinct Color), Make desc`,
			rows:     59,
			firstrow: `{"Make": "TSMR", "count": 1}`,
		},
		{
			// same query result as above, computed differently
			query:    `select count(*) from (select distinct Color from parking where Make = 'HOND')`,
			rows:     1,
			firstrow: countmsg(16),
		},
		{
			// test expressions containing aggregates
			query:    `select MAX(Ticket + 1) + MAX(PlateExpiry) AS out from parking`,
			rows:     1,
			firstrow: fmt.Sprintf(`{"out": %d}`, 4272473893+201905),
		},
		{
			query: `select sum(total_amount)-sum(fare_amount) as diff, payment_type from nyc_taxi group by payment_type order by diff desc`,
			expectedRows: []string{
				`{"diff": 4993.759996700002, "payment_type": "Credit"}`,
				`{"diff": 2475.2499420999957, "payment_type": "CASH"}`,
				`{"diff": 93.1000019, "payment_type": "CREDIT"}`,
				`{"diff": 59.14999400000033, "payment_type": "Cash"}`,
				`{"diff": 0, "payment_type": "No Charge"}`,
				`{"diff": 0, "payment_type": "Dispute"}`,
			},
		},
		{
			// semantically the same query as above;
			// we should get the same results...
			query: `select sum(total_amount-fare_amount) as diff, payment_type from nyc_taxi group by payment_type order by diff desc`,
			rows:  6, // can confirm with 'select count(distinct payment_type) ...'
			expectedRows: []string{
				`{"payment_type": "Credit", "diff": 4993.759996700001}`,
				`{"payment_type": "CASH", "diff": 2475.2499421}`,
				`{"payment_type": "CREDIT", "diff": 93.1000019}`,
				`{"payment_type": "Cash", "diff": 59.14999399999999}`,
				`{"payment_type": "No Charge", "diff": 0}`,
				`{"payment_type": "Dispute", "diff": 0}`,
			},
		},
		{
			// test simple ORDER BY clause (IssueTime forces order of rows Make="CHEV")
			query: `select Ticket, IssueTime, Make from parking
                       where ViolationCode like '80.69A+'
                       order by Make desc, IssueTime LIMIT 10`,
			expectedRows: []string{
				`{"Ticket": 4272473866, "IssueTime": 1510, "Make": "TOYT"}`,
				`{"Ticket": 4272349266, "IssueTime": 1615, "Make": "NISS"}`,
				`{"Ticket": 4272473870, "IssueTime": 1511, "Make": "HOND"}`,
				`{"Ticket": 4272473881, "IssueTime": 1512, "Make": "CHEV"}`,
				`{"Ticket": 4272349270, "IssueTime": 1623, "Make": "CHEV"}`,
			},
		},
		{
			// simple scalar sub-query search
			query:    `select PlateExpiry, Make, BodyStyle from parking where Ticket = (select max(Ticket) from parking)`,
			rows:     1,
			firstrow: `{"PlateExpiry": 201506, "Make": "NISS", "BodyStyle": "PU"}`,
		},
		{
			// more complex sub-query search;
			// equivalent to
			// select sum(FINE) from parking where Make is not missing
			query:    `select sum(Fine) from parking where Make in (select distinct Make from parking)`,
			rows:     1,
			firstrow: `{"sum": 71016}`,
		},
		{
			// select sum(Fine)
			// from records where Make
			// is one of the top 5 Makes by occurence
			query: `
select sum(Fine)
from parking
where Make in (
  select Make
  from (
    select count(Make), Make
    from parking
    group by Make
    order by count(Make) desc
    limit 5
  )
)
`,
			rows:     1,
			firstrow: `{"sum": 33756}`,
		},
		{
			// test ORDER BY an experession and field
			query: `select Ticket, IssueTime, Make from parking
                       where ViolationCode like '80.69A+'
                       order by Make desc, -1*IssueTime LIMIT 6`,
			expectedRows: []string{
				`{"Ticket": 4272473866, "IssueTime": 1510, "Make": "TOYT"}`,
				`{"Ticket": 4272349266, "IssueTime": 1615, "Make": "NISS"}`,
				`{"Ticket": 4272473870, "IssueTime": 1511, "Make": "HOND"}`,
				`{"Ticket": 4272349270, "IssueTime": 1623, "Make": "CHEV"}`,
				`{"Ticket": 4272473881, "IssueTime": 1512, "Make": "CHEV"}`,
			},
		},
		{
			// test ORDER BY clause with LIMIT
			query: `select Ticket, NULL as nil from parking order by Ticket limit 4`,
			expectedRows: []string{
				`{"Ticket": 1103341116, "nil": null}`,
				`{"Ticket": 1103700150, "nil": null}`,
				`{"Ticket": 1104803000, "nil": null}`,
				`{"Ticket": 1104820732, "nil": null}`,
			},
		},
		{
			// test ORDER BY clause with LIMIT and OFFSET
			query: `select Ticket from parking order by Ticket limit 2 offset 2`,
			expectedRows: []string{
				`{"Ticket": 1104803000}`,
				`{"Ticket": 1104820732}`,
			},
		},
		{
			// test projection of a computed number
			// that is sometimes an integer and sometimes a float
			query: `select Ticket as t, Ticket/2 as half from parking order by Ticket limit 16`,
			expectedRows: []string{
				`{"t": 1103341116, "half": 551670558}`,
				`{"t": 1103700150, "half": 551850075}`,
				`{"t": 1104803000, "half": 552401500}`,
				`{"t": 1104820732, "half": 552410366}`,
				`{"t": 1105461453, "half": 552730726.5}`,
				`{"t": 1106226590, "half": 553113295}`,
				`{"t": 1106500452, "half": 553250226}`,
				`{"t": 1106500463, "half": 553250231.5}`,
				`{"t": 1106506402, "half": 553253201}`,
				`{"t": 1106506413, "half": 553253206.5}`,
				`{"t": 1106506424, "half": 553253212}`,
				`{"t": 1106506435, "half": 553253217.5}`,
				`{"t": 1106506446, "half": 553253223}`,
				`{"t": 1106549754, "half": 553274877}`,
				`{"t": 1107179581, "half": 553589790.5}`,
				`{"t": 1107179592, "half": 553589796}`,
			},
		},
		{
			// test projection where some of the integers
			// have eight significant bytes
			// (and test that negation is performed correctly)
			query: `select Ticket as t, Ticket*-1024*1024*512 as big from parking order by Ticket limit 16`,
			expectedRows: []string{
				// note: some of these results are not precise
				// due to the intermediate result being computed
				// using floating-point doubles; the point of the test here
				// is just to confirm that 9-byte integers are scattered correctly
				`{"t": 1103341116, "big": -592351751194017792}`,
				`{"t": 1103700150, "big": -592544506105036800}`,
				`{"t": 1104803000, "big": -593136594190336000}`,
				`{"t": 1104820732, "big": -593146113985347584}`,
				`{"t": 1105461453, "big": -593490098452955136}`,
				`{"t": 1106226590, "big": -593900878251950080}`,
				`{"t": 1106500452, "big": -594047906793652224}`,
				`{"t": 1106500463, "big": -594047912699232256}`,
				`{"t": 1106506402, "big": -594051101175578624}`,
				`{"t": 1106506413, "big": -594051107081158656}`,
				`{"t": 1106506424, "big": -594051112986738688}`,
				`{"t": 1106506435, "big": -594051118892318720}`,
				`{"t": 1106506446, "big": -594051124797898752}`,
				`{"t": 1106549754, "big": -594074375603355648}`,
				`{"t": 1107179581, "big": -594412511399247872}`,
				`{"t": 1107179592, "big": -594412517304827904}`,
			},
		},
		{
			query: `select Ticket as t from parking where 1 > 2`,
			rows:  0,
			matchPlan: []string{
				"NONE",
			},
			expectedRows: []string{},
		},
		// Test aggregation with multiple GROUP BY fields.
		{
			query: `SELECT MAX(Ticket), Route FROM parking GROUP BY Route`,
			rows:  138,
		},
		{
			query: `SELECT MAX(Ticket), Route, RPState FROM parking GROUP BY Route, RPState`,
			rows:  184,
		},
		{
			query: `SELECT MAX(Ticket), Route, RPState, Location FROM parking GROUP BY Route, RPState, Location`,
			rows:  861,
		},
		// Test aggregation with expressions in GROUP BY.
		{
			query: `SELECT MAX(Ticket), FLOOR(Fine / 10000) FROM parking GROUP BY FLOOR(Fine / 10000)`,
			rows:  1,
		},
		{
			query: `SELECT MAX(Ticket) FROM parking GROUP BY FLOOR(Fine / 10000)`,
			rows:  1,
		},
		{
			query: `select 3 AS x, 'foo' AS y`,
			expectedRows: []string{
				`{"x": 3, "y": "foo"}`,
			},
		},
		{
			query: `select Ticket as t, Make as m, (select size(p3.Entries) from parking3 p3 where p3.Make = m limit 1) as num from parking limit 10`,
			expectedRows: []string{
				`{"t": 1103341116, "m": "HOND", "num": 122}`,
				`{"t": 1103700150, "m": "GMC", "num": 18}`,
				`{"t": 1104803000, "m": "NISS", "num": 80}`,
				`{"t": 1104820732, "m": "ACUR", "num": 15}`,
				`{"t": 1105461453, "m": "CHEV", "num": 70}`,
				`{"t": 1106226590, "m": "CHEV", "num": 70}`,
				`{"t": 1106500452, "m": "MAZD", "num": 15}`,
				`{"t": 1106500463, "m": "TOYO", "num": 96}`,
				`{"t": 1106506402, "m": "CHEV", "num": 70}`,
				`{"t": 1106506413, "m": "NISS", "num": 80}`,
			},
		},
		{
			query: `select count(*) from parking ++ nyc_taxi`,
			expectedRows: []string{
				`{"count": 9583}`,
			},
		},
		{
			query: `select earliest(foo), latest(foo) from parking ++ nyc_taxi`,
			indexer: testindexer{
				"parking": testindex{
					"foo": dates(
						"2000-01-01T00:00:00Z",
						"2000-02-01T00:00:00Z",
					),
				},
				"nyc_taxi": testindex{
					"foo": dates(
						"2000-02-01T00:00:00Z",
						"2000-03-01T00:00:00Z",
					),
				},
			},
			rows: 1,
			matchPlan: []string{
				"PROJECT `2000-01-01T00:00:00Z` AS \"min\", `2000-03-01T00:00:00Z` AS \"max\"",
			},
		},
		{
			query: `select earliest(foo), latest(foo) from table_glob("*a*")`,
			indexer: testindexer{
				"parking": testindex{
					"foo": dates(
						"2000-01-01T00:00:00Z",
						"2000-02-01T00:00:00Z",
					),
				},
				"nyc_taxi": testindex{
					"foo": dates(
						"2000-02-01T00:00:00Z",
						"2000-03-01T00:00:00Z",
					),
				},
			},
			rows: 1,
			matchPlan: []string{
				"PROJECT `2000-01-01T00:00:00Z` AS \"min\", `2000-03-01T00:00:00Z` AS \"max\"",
			},
		},
		{
			// multiple references to the same table
			// should be merged into a single input,
			// but inputs used as replacements should
			// not be merged
			query: `select a, (select b from parking limit 1), (select c from parking limit 1) from parking where d = (select IssueData from parking limit 1)`,
			matchPlan: []string{
				`WITH REPLACEMENT\(0\) AS \(`,
				`	parking`,
				`	LIMIT 1`,
				`	PROJECT b AS b`,
				`\)`,
				`WITH REPLACEMENT\(1\) AS \(`,
				`	parking`,
				`	LIMIT 1`,
				`	PROJECT c AS c`,
				`\)`,
				`WITH REPLACEMENT\(2\) AS \(`,
				`	parking`,
				`	LIMIT 1`,
				`	PROJECT IssueData AS IssueData`,
				`\)`,
				`parking`,
				`WHERE d = SCALAR_REPLACEMENT\(2\)`,
				`PROJECT a AS a, SCALAR_REPLACEMENT\(0\) AS _2, SCALAR_REPLACEMENT\(1\) AS _3`,
			},
		},
		{
			query: `SELECT APPROX_COUNT_DISTINCT(Make) AS "count", APPROX_COUNT_DISTINCT(foo) AS "count2" FROM parking3`,
			expectedRows: []string{
				`{"count": 59, "count2": 0}`,
			},
		},
		{
			query: `SELECT Color, APPROX_COUNT_DISTINCT(Make) FROM parking GROUP BY Color ORDER BY Color LIMIT 5`,
			expectedRows: []string{
				`{"Color": "BG", "count": 2}`,
				`{"Color": "BK", "count": 35}`,
				`{"Color": "BL", "count": 23}`,
				`{"Color": "BN", "count": 4}`,
				`{"Color": "BR", "count": 3}`,
			},
		},

		{
			query: `SELECT cols FROM UNPIVOT parking AT cols GROUP BY cols ORDER BY cols LIMIT 100`,
			expectedRows: []string{
				`{"cols": "Agency"}`,
				`{"cols": "BodyStyle"}`,
				`{"cols": "Color"}`,
				`{"cols": "Fine"}`,
				`{"cols": "IssueData"}`,
				`{"cols": "IssueTime"}`,
				`{"cols": "Latitude"}`,
				`{"cols": "Location"}`,
				`{"cols": "Longitude"}`,
				`{"cols": "Make"}`,
				`{"cols": "MarkedTime"}`,
				`{"cols": "MeterId"}`,
				`{"cols": "PlateExpiry"}`,
				`{"cols": "RPState"}`,
				`{"cols": "Route"}`,
				`{"cols": "Ticket"}`,
				`{"cols": "ViolationCode"}`,
				`{"cols": "ViolationDescr"}`,
			},
		},
		{
			query: `EXPLAIN WITH main AS (SELECT * FROM nyc_taxi) SELECT COUNT(*) FROM main`,
			rows:  1,
			matchPlan: []string{
				"EXPLAIN QUERY",
			},
		},
		{
			query: `SELECT sneller_datashape(*) FROM parking`,
			expectedRows: []string{
				`{"total": 1023, "fields": {"ViolationCode": {"string": 1023, "string-min-length": 3, "string-max-length": 8}, "Longitude": {"int": 1023, "int-min-value": 99999, "int-max-value": 99999}, "Fine": {"int": 1012, "int-min-value": 25, "int-max-value": 363}, "ViolationDescr": {"string": 1023, "string-min-length": 4, "string-max-length": 20}, "Agency": {"int": 1023, "int-min-value": 1, "int-max-value": 57}, "PlateExpiry": {"int": 956, "int-min-value": 1, "int-max-value": 201905}, "BodyStyle": {"string": 1015, "string-min-length": 2, "string-max-length": 2}, "Ticket": {"int": 1023, "int-min-value": 1103341116, "int-max-value": 4272473892}, "Location": {"string": 1022, "string-min-length": 7, "string-max-length": 31}, "Color": {"string": 1016, "string-min-length": 2, "string-max-length": 2}, "MeterId": {"string": 125, "string-min-length": 2, "string-max-length": 7}, "Route": {"string": 1001, "string-min-length": 2, "string-max-length": 5}, "IssueData": {"timestamp": 1023}, "MarkedTime": {"string": 7, "string-min-length": 4, "string-max-length": 4}, "Latitude": {"int": 1023, "int-min-value": 99999, "int-max-value": 99999}, "Make": {"string": 1019, "string-min-length": 2, "string-max-length": 4}, "RPState": {"string": 1023, "string-min-length": 2, "string-max-length": 2}, "IssueTime": {"int": 1022, "int-min-value": 18, "int-max-value": 2355}}}`,
			},
		},
		{
			query: `SELECT Make, Color, COUNT(*), ROW_NUMBER() OVER (PARTITION BY Make ORDER BY COUNT(*) DESC) FROM parking GROUP BY Make, Color ORDER BY Make, Color`,
			expectedRows: []string{
				`{"Make": "ACUR", "Color": "BK", "count": 3, "row_number": 3}`,
				`{"Make": "ACUR", "Color": "GN", "count": 1, "row_number": 7}`,
				`{"Make": "ACUR", "Color": "GY", "count": 4, "row_number": 1}`,
				`{"Make": "ACUR", "Color": "RD", "count": 1, "row_number": 6}`,
				`{"Make": "ACUR", "Color": "SI", "count": 3, "row_number": 2}`,
				`{"Make": "ACUR", "Color": "SL", "count": 2, "row_number": 4}`,
				`{"Make": "ACUR", "Color": "WH", "count": 1, "row_number": 5}`,
				`{"Make": "AUDI", "Color": "BK", "count": 5, "row_number": 1}`,
				`{"Make": "AUDI", "Color": "GY", "count": 3, "row_number": 2}`,
				`{"Make": "AUDI", "Color": "SI", "count": 1, "row_number": 5}`,
				`{"Make": "AUDI", "Color": "WH", "count": 1, "row_number": 4}`,
				`{"Make": "AUDI", "Color": "WT", "count": 2, "row_number": 3}`,
				`{"Make": "BENZ", "Color": "BK", "count": 2, "row_number": 1}`,
				`{"Make": "BENZ", "Color": "WH", "count": 1, "row_number": 2}`,
				`{"Make": "BMW", "Color": "BK", "count": 13, "row_number": 1}`,
				`{"Make": "BMW", "Color": "BL", "count": 4, "row_number": 5}`,
				`{"Make": "BMW", "Color": "GR", "count": 1, "row_number": 9}`,
				`{"Make": "BMW", "Color": "GY", "count": 10, "row_number": 2}`,
				`{"Make": "BMW", "Color": "MR", "count": 2, "row_number": 8}`,
				`{"Make": "BMW", "Color": "RE", "count": 1, "row_number": 10}`,
				`{"Make": "BMW", "Color": "SI", "count": 3, "row_number": 7}`,
				`{"Make": "BMW", "Color": "SL", "count": 3, "row_number": 6}`,
				`{"Make": "BMW", "Color": "WH", "count": 6, "row_number": 4}`,
				`{"Make": "BMW", "Color": "WT", "count": 6, "row_number": 3}`,
				`{"Make": "BUIC", "Color": "RD", "count": 1, "row_number": 3}`,
				`{"Make": "BUIC", "Color": "WH", "count": 1, "row_number": 2}`,
				`{"Make": "BUIC", "Color": "WT", "count": 2, "row_number": 1}`,
				`{"Make": "CADI", "Color": "BK", "count": 5, "row_number": 1}`,
				`{"Make": "CADI", "Color": "BL", "count": 1, "row_number": 3}`,
				`{"Make": "CADI", "Color": "OT", "count": 1, "row_number": 4}`,
				`{"Make": "CADI", "Color": "WT", "count": 1, "row_number": 2}`,
				`{"Make": "CHEC", "Color": "OT", "count": 1, "row_number": 1}`,
				`{"Make": "CHEV", "Color": "BK", "count": 14, "row_number": 1}`,
				`{"Make": "CHEV", "Color": "BL", "count": 7, "row_number": 5}`,
				`{"Make": "CHEV", "Color": "BR", "count": 1, "row_number": 14}`,
				`{"Make": "CHEV", "Color": "GN", "count": 2, "row_number": 11}`,
				`{"Make": "CHEV", "Color": "GY", "count": 12, "row_number": 2}`,
				`{"Make": "CHEV", "Color": "MA", "count": 2, "row_number": 9}`,
				`{"Make": "CHEV", "Color": "PR", "count": 1, "row_number": 13}`,
				`{"Make": "CHEV", "Color": "RD", "count": 4, "row_number": 7}`,
				`{"Make": "CHEV", "Color": "RE", "count": 3, "row_number": 8}`,
				`{"Make": "CHEV", "Color": "SI", "count": 2, "row_number": 10}`,
				`{"Make": "CHEV", "Color": "SL", "count": 8, "row_number": 3}`,
				`{"Make": "CHEV", "Color": "WH", "count": 8, "row_number": 4}`,
				`{"Make": "CHEV", "Color": "WT", "count": 5, "row_number": 6}`,
				`{"Make": "CHEV", "Color": "YE", "count": 1, "row_number": 12}`,
				`{"Make": "CHRY", "Color": "BG", "count": 1, "row_number": 4}`,
				`{"Make": "CHRY", "Color": "BK", "count": 2, "row_number": 3}`,
				`{"Make": "CHRY", "Color": "BL", "count": 1, "row_number": 5}`,
				`{"Make": "CHRY", "Color": "GN", "count": 1, "row_number": 7}`,
				`{"Make": "CHRY", "Color": "GO", "count": 1, "row_number": 6}`,
				`{"Make": "CHRY", "Color": "GY", "count": 3, "row_number": 1}`,
				`{"Make": "CHRY", "Color": "RE", "count": 1, "row_number": 8}`,
				`{"Make": "CHRY", "Color": "SL", "count": 2, "row_number": 2}`,
				`{"Make": "CHRY", "Color": "WH", "count": 1, "row_number": 9}`,
				`{"Make": "CHRY", "Color": "WT", "count": 1, "row_number": 10}`,
				`{"Make": "DODG", "Color": "BK", "count": 5, "row_number": 3}`,
				`{"Make": "DODG", "Color": "BL", "count": 2, "row_number": 9}`,
				`{"Make": "DODG", "Color": "GN", "count": 1, "row_number": 11}`,
				`{"Make": "DODG", "Color": "GR", "count": 2, "row_number": 10}`,
				`{"Make": "DODG", "Color": "GY", "count": 6, "row_number": 2}`,
				`{"Make": "DODG", "Color": "MR", "count": 2, "row_number": 8}`,
				`{"Make": "DODG", "Color": "RE", "count": 3, "row_number": 6}`,
				`{"Make": "DODG", "Color": "SI", "count": 2, "row_number": 7}`,
				`{"Make": "DODG", "Color": "SL", "count": 5, "row_number": 5}`,
				`{"Make": "DODG", "Color": "WH", "count": 5, "row_number": 4}`,
				`{"Make": "DODG", "Color": "WT", "count": 6, "row_number": 1}`,
				`{"Make": "FIAT", "Color": "GY", "count": 2, "row_number": 1}`,
				`{"Make": "FIAT", "Color": "OR", "count": 1, "row_number": 3}`,
				`{"Make": "FIAT", "Color": "RD", "count": 1, "row_number": 2}`,
				`{"Make": "FIAT", "Color": "SL", "count": 1, "row_number": 4}`,
				`{"Make": "FORD", "Color": "BG", "count": 1, "row_number": 12}`,
				`{"Make": "FORD", "Color": "BK", "count": 12, "row_number": 3}`,
				`{"Make": "FORD", "Color": "BL", "count": 6, "row_number": 6}`,
				`{"Make": "FORD", "Color": "GN", "count": 4, "row_number": 7}`,
				`{"Make": "FORD", "Color": "GO", "count": 2, "row_number": 10}`,
				`{"Make": "FORD", "Color": "GR", "count": 1, "row_number": 11}`,
				`{"Make": "FORD", "Color": "GY", "count": 11, "row_number": 5}`,
				`{"Make": "FORD", "Color": "RD", "count": 4, "row_number": 8}`,
				`{"Make": "FORD", "Color": "RE", "count": 4, "row_number": 9}`,
				`{"Make": "FORD", "Color": "SI", "count": 1, "row_number": 13}`,
				`{"Make": "FORD", "Color": "SL", "count": 11, "row_number": 4}`,
				`{"Make": "FORD", "Color": "WH", "count": 13, "row_number": 2}`,
				`{"Make": "FORD", "Color": "WT", "count": 16, "row_number": 1}`,
				`{"Make": "FORD", "Color": "YE", "count": 1, "row_number": 14}`,
				`{"Make": "FREI", "Color": "RE", "count": 1, "row_number": 1}`,
				`{"Make": "FRHT", "Color": "BL", "count": 1, "row_number": 2}`,
				`{"Make": "FRHT", "Color": "BN", "count": 1, "row_number": 1}`,
				`{"Make": "GMC", "Color": "BK", "count": 8, "row_number": 1}`,
				`{"Make": "GMC", "Color": "BL", "count": 1, "row_number": 7}`,
				`{"Make": "GMC", "Color": "GY", "count": 1, "row_number": 8}`,
				`{"Make": "GMC", "Color": "MR", "count": 2, "row_number": 4}`,
				`{"Make": "GMC", "Color": "SI", "count": 1, "row_number": 5}`,
				`{"Make": "GMC", "Color": "SL", "count": 2, "row_number": 3}`,
				`{"Make": "GMC", "Color": "WH", "count": 2, "row_number": 2}`,
				`{"Make": "GMC", "Color": "WT", "count": 1, "row_number": 6}`,
				`{"Make": "HINO", "Color": "WH", "count": 1, "row_number": 1}`,
				`{"Make": "HOND", "Color": "BK", "count": 24, "row_number": 2}`,
				`{"Make": "HOND", "Color": "BL", "count": 5, "row_number": 7}`,
				`{"Make": "HOND", "Color": "BN", "count": 1, "row_number": 16}`,
				`{"Make": "HOND", "Color": "BR", "count": 1, "row_number": 14}`,
				`{"Make": "HOND", "Color": "GN", "count": 3, "row_number": 10}`,
				`{"Make": "HOND", "Color": "GO", "count": 3, "row_number": 9}`,
				`{"Make": "HOND", "Color": "GR", "count": 2, "row_number": 12}`,
				`{"Make": "HOND", "Color": "GY", "count": 32, "row_number": 1}`,
				`{"Make": "HOND", "Color": "MR", "count": 5, "row_number": 8}`,
				`{"Make": "HOND", "Color": "RD", "count": 2, "row_number": 13}`,
				`{"Make": "HOND", "Color": "RE", "count": 1, "row_number": 15}`,
				`{"Make": "HOND", "Color": "SI", "count": 7, "row_number": 6}`,
				`{"Make": "HOND", "Color": "SL", "count": 14, "row_number": 3}`,
				`{"Make": "HOND", "Color": "TA", "count": 2, "row_number": 11}`,
				`{"Make": "HOND", "Color": "WH", "count": 10, "row_number": 5}`,
				`{"Make": "HOND", "Color": "WT", "count": 10, "row_number": 4}`,
				`{"Make": "HYUN", "Color": "BK", "count": 8, "row_number": 1}`,
				`{"Make": "HYUN", "Color": "BL", "count": 5, "row_number": 3}`,
				`{"Make": "HYUN", "Color": "GY", "count": 7, "row_number": 2}`,
				`{"Make": "HYUN", "Color": "RE", "count": 1, "row_number": 8}`,
				`{"Make": "HYUN", "Color": "SI", "count": 3, "row_number": 6}`,
				`{"Make": "HYUN", "Color": "SL", "count": 2, "row_number": 7}`,
				`{"Make": "HYUN", "Color": "WH", "count": 4, "row_number": 4}`,
				`{"Make": "HYUN", "Color": "WT", "count": 3, "row_number": 5}`,
				`{"Make": "INFI", "Color": "BK", "count": 7, "row_number": 1}`,
				`{"Make": "INFI", "Color": "GY", "count": 5, "row_number": 2}`,
				`{"Make": "INFI", "Color": "SI", "count": 2, "row_number": 4}`,
				`{"Make": "INFI", "Color": "SL", "count": 1, "row_number": 5}`,
				`{"Make": "INFI", "Color": "WT", "count": 2, "row_number": 3}`,
				`{"Make": "ISU", "Color": "BK", "count": 1, "row_number": 1}`,
				`{"Make": "JAGR", "Color": "WH", "count": 1, "row_number": 1}`,
				`{"Make": "JAGU", "Color": "MR", "count": 1, "row_number": 1}`,
				`{"Make": "JEEP", "Color": "BK", "count": 7, "row_number": 1}`,
				`{"Make": "JEEP", "Color": "BL", "count": 1, "row_number": 10}`,
				`{"Make": "JEEP", "Color": "GO", "count": 1, "row_number": 8}`,
				`{"Make": "JEEP", "Color": "GR", "count": 2, "row_number": 5}`,
				`{"Make": "JEEP", "Color": "GY", "count": 4, "row_number": 2}`,
				`{"Make": "JEEP", "Color": "MA", "count": 1, "row_number": 6}`,
				`{"Make": "JEEP", "Color": "MR", "count": 1, "row_number": 12}`,
				`{"Make": "JEEP", "Color": "OT", "count": 1, "row_number": 9}`,
				`{"Make": "JEEP", "Color": "RD", "count": 2, "row_number": 4}`,
				`{"Make": "JEEP", "Color": "RE", "count": 1, "row_number": 14}`,
				`{"Make": "JEEP", "Color": "TA", "count": 1, "row_number": 11}`,
				`{"Make": "JEEP", "Color": "WH", "count": 1, "row_number": 7}`,
				`{"Make": "JEEP", "Color": "WT", "count": 3, "row_number": 3}`,
				`{"Make": "JEEP", "Color": "YE", "count": 1, "row_number": 13}`,
				`{"Make": "KIA", "Color": "BK", "count": 3, "row_number": 2}`,
				`{"Make": "KIA", "Color": "BL", "count": 1, "row_number": 6}`,
				`{"Make": "KIA", "Color": "GY", "count": 3, "row_number": 5}`,
				`{"Make": "KIA", "Color": "MA", "count": 1, "row_number": 8}`,
				`{"Make": "KIA", "Color": "MR", "count": 1, "row_number": 7}`,
				`{"Make": "KIA", "Color": "SI", "count": 1, "row_number": 10}`,
				`{"Make": "KIA", "Color": "SL", "count": 3, "row_number": 3}`,
				`{"Make": "KIA", "Color": "TA", "count": 1, "row_number": 9}`,
				`{"Make": "KIA", "Color": "WH", "count": 3, "row_number": 4}`,
				`{"Make": "KIA", "Color": "WT", "count": 5, "row_number": 1}`,
				`{"Make": "KW", "Color": "RE", "count": 1, "row_number": 1}`,
				`{"Make": "LEXS", "Color": "BK", "count": 6, "row_number": 1}`,
				`{"Make": "LEXS", "Color": "SL", "count": 2, "row_number": 2}`,
				`{"Make": "LEXS", "Color": "WT", "count": 1, "row_number": 3}`,
				`{"Make": "LEXU", "Color": "BK", "count": 7, "row_number": 1}`,
				`{"Make": "LEXU", "Color": "GO", "count": 1, "row_number": 5}`,
				`{"Make": "LEXU", "Color": "GY", "count": 5, "row_number": 2}`,
				`{"Make": "LEXU", "Color": "SI", "count": 1, "row_number": 4}`,
				`{"Make": "LEXU", "Color": "WH", "count": 4, "row_number": 3}`,
				`{"Make": "LINC", "Color": "BK", "count": 3, "row_number": 1}`,
				`{"Make": "LINC", "Color": "BL", "count": 1, "row_number": 4}`,
				`{"Make": "LINC", "Color": "GY", "count": 2, "row_number": 2}`,
				`{"Make": "LINC", "Color": "RD", "count": 1, "row_number": 5}`,
				`{"Make": "LINC", "Color": "TN", "count": 1, "row_number": 3}`,
				`{"Make": "LIND", "Color": "TA", "count": 1, "row_number": 1}`,
				`{"Make": "LROV", "Color": "BK", "count": 2, "row_number": 1}`,
				`{"Make": "LROV", "Color": "GY", "count": 1, "row_number": 2}`,
				`{"Make": "LROV", "Color": "RE", "count": 1, "row_number": 3}`,
				`{"Make": "MASE", "Color": "BL", "count": 1, "row_number": 1}`,
				`{"Make": "MAZD", "Color": "BK", "count": 2, "row_number": 2}`,
				`{"Make": "MAZD", "Color": "BL", "count": 2, "row_number": 3}`,
				`{"Make": "MAZD", "Color": "GN", "count": 1, "row_number": 8}`,
				`{"Make": "MAZD", "Color": "GO", "count": 1, "row_number": 4}`,
				`{"Make": "MAZD", "Color": "GY", "count": 3, "row_number": 1}`,
				`{"Make": "MAZD", "Color": "MA", "count": 1, "row_number": 11}`,
				`{"Make": "MAZD", "Color": "MR", "count": 1, "row_number": 5}`,
				`{"Make": "MAZD", "Color": "RD", "count": 1, "row_number": 6}`,
				`{"Make": "MAZD", "Color": "RE", "count": 1, "row_number": 7}`,
				`{"Make": "MAZD", "Color": "SI", "count": 1, "row_number": 9}`,
				`{"Make": "MAZD", "Color": "SL", "count": 1, "row_number": 10}`,
				`{"Make": "MBNZ", "Color": "BK", "count": 3, "row_number": 3}`,
				`{"Make": "MBNZ", "Color": "GY", "count": 5, "row_number": 2}`,
				`{"Make": "MBNZ", "Color": "WH", "count": 6, "row_number": 1}`,
				`{"Make": "MERC", "Color": "BL", "count": 1, "row_number": 2}`,
				`{"Make": "MERC", "Color": "WH", "count": 1, "row_number": 1}`,
				`{"Make": "MERC", "Color": "WT", "count": 1, "row_number": 3}`,
				`{"Make": "MERZ", "Color": "BK", "count": 8, "row_number": 1}`,
				`{"Make": "MERZ", "Color": "BL", "count": 1, "row_number": 4}`,
				`{"Make": "MERZ", "Color": "GY", "count": 7, "row_number": 2}`,
				`{"Make": "MERZ", "Color": "SL", "count": 2, "row_number": 3}`,
				`{"Make": "MITS", "Color": "BK", "count": 1, "row_number": 4}`,
				`{"Make": "MITS", "Color": "BL", "count": 1, "row_number": 7}`,
				`{"Make": "MITS", "Color": "GY", "count": 3, "row_number": 1}`,
				`{"Make": "MITS", "Color": "OR", "count": 1, "row_number": 6}`,
				`{"Make": "MITS", "Color": "RE", "count": 1, "row_number": 5}`,
				`{"Make": "MITS", "Color": "SI", "count": 2, "row_number": 2}`,
				`{"Make": "MITS", "Color": "WT", "count": 2, "row_number": 3}`,
				`{"Make": "MNNI", "Color": "BK", "count": 1, "row_number": 3}`,
				`{"Make": "MNNI", "Color": "BL", "count": 2, "row_number": 1}`,
				`{"Make": "MNNI", "Color": "BN", "count": 2, "row_number": 2}`,
				`{"Make": "NISS", "Color": "BK", "count": 19, "row_number": 1}`,
				`{"Make": "NISS", "Color": "BL", "count": 7, "row_number": 4}`,
				`{"Make": "NISS", "Color": "BN", "count": 1, "row_number": 11}`,
				`{"Make": "NISS", "Color": "GN", "count": 3, "row_number": 8}`,
				`{"Make": "NISS", "Color": "GR", "count": 2, "row_number": 9}`,
				`{"Make": "NISS", "Color": "GY", "count": 15, "row_number": 2}`,
				`{"Make": "NISS", "Color": "MA", "count": 1, "row_number": 12}`,
				`{"Make": "NISS", "Color": "MR", "count": 1, "row_number": 13}`,
				`{"Make": "NISS", "Color": "OR", "count": 1, "row_number": 14}`,
				`{"Make": "NISS", "Color": "RD", "count": 2, "row_number": 10}`,
				`{"Make": "NISS", "Color": "RE", "count": 1, "row_number": 15}`,
				`{"Make": "NISS", "Color": "SI", "count": 6, "row_number": 6}`,
				`{"Make": "NISS", "Color": "SL", "count": 5, "row_number": 7}`,
				`{"Make": "NISS", "Color": "TA", "count": 1, "row_number": 16}`,
				`{"Make": "NISS", "Color": "WH", "count": 6, "row_number": 5}`,
				`{"Make": "NISS", "Color": "WT", "count": 8, "row_number": 3}`,
				`{"Make": "OLDS", "Color": "BU", "count": 1, "row_number": 1}`,
				`{"Make": "OLDS", "Color": "GN", "count": 1, "row_number": 2}`,
				`{"Make": "OTHR", "Color": "WT", "count": 3, "row_number": 1}`,
				`{"Make": "OTHR", "Color": "YE", "count": 1, "row_number": 2}`,
				`{"Make": "PLYM", "Color": "WT", "count": 2, "row_number": 1}`,
				`{"Make": "PONT", "Color": "GR", "count": 1, "row_number": 3}`,
				`{"Make": "PONT", "Color": "GY", "count": 1, "row_number": 6}`,
				`{"Make": "PONT", "Color": "RD", "count": 1, "row_number": 5}`,
				`{"Make": "PONT", "Color": "SI", "count": 1, "row_number": 2}`,
				`{"Make": "PONT", "Color": "SL", "count": 2, "row_number": 1}`,
				`{"Make": "PONT", "Color": "WT", "count": 1, "row_number": 4}`,
				`{"Make": "PORS", "Color": "BK", "count": 1, "row_number": 2}`,
				`{"Make": "PORS", "Color": "GY", "count": 2, "row_number": 1}`,
				`{"Make": "PTRB", "Color": "BK", "count": 1, "row_number": 2}`,
				`{"Make": "PTRB", "Color": "RD", "count": 2, "row_number": 1}`,
				`{"Make": "RROV", "Color": "GY", "count": 1, "row_number": 1}`,
				`{"Make": "SAA", "Color": "WT", "count": 1, "row_number": 1}`,
				`{"Make": "SATU", "Color": "BL", "count": 2, "row_number": 1}`,
				`{"Make": "SATU", "Color": "GR", "count": 1, "row_number": 2}`,
				`{"Make": "SCIO", "Color": "BK", "count": 1, "row_number": 2}`,
				`{"Make": "SCIO", "Color": "WH", "count": 1, "row_number": 1}`,
				`{"Make": "STRN", "Color": "BK", "count": 1, "row_number": 1}`,
				`{"Make": "STRN", "Color": "WT", "count": 1, "row_number": 2}`,
				`{"Make": "SUBA", "Color": "BK", "count": 2, "row_number": 3}`,
				`{"Make": "SUBA", "Color": "GO", "count": 1, "row_number": 5}`,
				`{"Make": "SUBA", "Color": "GY", "count": 3, "row_number": 2}`,
				`{"Make": "SUBA", "Color": "MA", "count": 1, "row_number": 4}`,
				`{"Make": "SUBA", "Color": "SI", "count": 5, "row_number": 1}`,
				`{"Make": "SUBA", "Color": "TA", "count": 1, "row_number": 6}`,
				`{"Make": "SUZI", "Color": "BK", "count": 1, "row_number": 1}`,
				`{"Make": "SUZU", "Color": "MA", "count": 1, "row_number": 2}`,
				`{"Make": "SUZU", "Color": "SI", "count": 2, "row_number": 1}`,
				`{"Make": "TESL", "Color": "GY", "count": 1, "row_number": 1}`,
				`{"Make": "TOYO", "Color": "BK", "count": 21, "row_number": 2}`,
				`{"Make": "TOYO", "Color": "BL", "count": 4, "row_number": 6}`,
				`{"Make": "TOYO", "Color": "BR", "count": 1, "row_number": 10}`,
				`{"Make": "TOYO", "Color": "GR", "count": 1, "row_number": 9}`,
				`{"Make": "TOYO", "Color": "GY", "count": 29, "row_number": 1}`,
				`{"Make": "TOYO", "Color": "RE", "count": 7, "row_number": 5}`,
				`{"Make": "TOYO", "Color": "SI", "count": 13, "row_number": 4}`,
				`{"Make": "TOYO", "Color": "TA", "count": 2, "row_number": 8}`,
				`{"Make": "TOYO", "Color": "WH", "count": 14, "row_number": 3}`,
				`{"Make": "TOYO", "Color": "YE", "count": 3, "row_number": 7}`,
				`{"Make": "TOYT", "Color": "BK", "count": 15, "row_number": 3}`,
				`{"Make": "TOYT", "Color": "BL", "count": 7, "row_number": 5}`,
				`{"Make": "TOYT", "Color": "GN", "count": 1, "row_number": 8}`,
				`{"Make": "TOYT", "Color": "GO", "count": 2, "row_number": 6}`,
				`{"Make": "TOYT", "Color": "GY", "count": 20, "row_number": 2}`,
				`{"Make": "TOYT", "Color": "MR", "count": 1, "row_number": 9}`,
				`{"Make": "TOYT", "Color": "RD", "count": 2, "row_number": 7}`,
				`{"Make": "TOYT", "Color": "SL", "count": 14, "row_number": 4}`,
				`{"Make": "TOYT", "Color": "WT", "count": 21, "row_number": 1}`,
				`{"Make": "TSMR", "Color": "GY", "count": 1, "row_number": 1}`,
				`{"Make": "UNK", "Color": "GY", "count": 1, "row_number": 2}`,
				`{"Make": "UNK", "Color": "RD", "count": 2, "row_number": 1}`,
				`{"Make": "VOLK", "Color": "BK", "count": 5, "row_number": 2}`,
				`{"Make": "VOLK", "Color": "BL", "count": 4, "row_number": 3}`,
				`{"Make": "VOLK", "Color": "GN", "count": 1, "row_number": 11}`,
				`{"Make": "VOLK", "Color": "GO", "count": 1, "row_number": 12}`,
				`{"Make": "VOLK", "Color": "GR", "count": 1, "row_number": 9}`,
				`{"Make": "VOLK", "Color": "GY", "count": 12, "row_number": 1}`,
				`{"Make": "VOLK", "Color": "OT", "count": 2, "row_number": 6}`,
				`{"Make": "VOLK", "Color": "RD", "count": 1, "row_number": 10}`,
				`{"Make": "VOLK", "Color": "SI", "count": 1, "row_number": 8}`,
				`{"Make": "VOLK", "Color": "SL", "count": 2, "row_number": 7}`,
				`{"Make": "VOLK", "Color": "WH", "count": 3, "row_number": 4}`,
				`{"Make": "VOLK", "Color": "WT", "count": 3, "row_number": 5}`,
				`{"Make": "VOLV", "Color": "BK", "count": 1, "row_number": 2}`,
				`{"Make": "VOLV", "Color": "RD", "count": 1, "row_number": 3}`,
				`{"Make": "VOLV", "Color": "RE", "count": 1, "row_number": 4}`,
				`{"Make": "VOLV", "Color": "SL", "count": 3, "row_number": 1}`,
			},
		},
	}

	for i := range tcs {
		if len(tcs[i].expectedRows) > 0 && tcs[i].rows == 0 {
			tcs[i].rows = len(tcs[i].expectedRows)
		}

		if len(tcs[i].expectedRows) == 0 && tcs[i].firstrow != "" {
			tcs[i].expectedRows = append(tcs[i].expectedRows, tcs[i].firstrow)
		}

		text := tcs[i].query
		schema := tcs[i].schema
		indexer := tcs[i].indexer
		pmatch := tcs[i].matchPlan
		scanned := tcs[i].expectBytes
		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			var dst bytes.Buffer
			q, err := partiql.Parse([]byte(text))
			if err != nil {
				t.Logf("parsing %q", text)
				t.Fatal(err)
			}
			// test that this parsed query
			// is equivalent to itself when
			// marshalled and unmarshalled
			t.Run("serialize", func(t *testing.T) {
				testSerialize(t, q.Body)
			})

			t.Logf("query: %s", expr.ToString(q))
			env.schema = schema
			env.indexer = indexer
			tree, err := New(q, env)
			if err != nil {
				t.Errorf("case %d: %s", i, err)
				return
			}
			planstr := tree.String()
			t.Logf("plan:\n%s", planstr)
			for i := range pmatch {
				m, err := regexp.MatchString(pmatch[i], planstr)
				if err != nil {
					t.Fatalf("bad regexp: %s", err)
				}
				if !m {
					t.Errorf("plan did not match pattern %q", pmatch[i])
				}
			}

			t.Run("serialize-plan", func(t *testing.T) {
				testPlanSerialize(t, tree)
			})

			dst.Reset()
			vm.Errorf = t.Logf
			defer func() {
				vm.Errorf = nil
			}()
			ep := &ExecParams{
				Plan:   tree,
				Output: &dst,
				Runner: env,
			}
			err = Exec(ep)
			if err != nil {
				t.Errorf("case %d: Exec: %s", i, err)
				return
			}
			if got := rowcount(t, dst.Bytes()); got != tcs[i].rows {
				t.Errorf("got %d rows instead of %d", got, tcs[i].rows)
			}
			if scanned != 0 && ep.Stats.BytesScanned != int64(scanned) {
				t.Errorf("scanned %d bytes; expected %d", ep.Stats.BytesScanned, scanned)
			}
			// test that the remote equivalent of this plan
			// produces exactly identical results
			t.Run("remote", func(t *testing.T) {
				testRemoteEquivalent(t, tree, env, dst.Bytes(), &ep.Stats)
			})
			t.Run("split", func(t *testing.T) {
				vm.Errorf = t.Logf
				defer func() {
					vm.Errorf = nil
				}()

				testSplitEquivalent(t, text, env, tcs[i].expectedRows, &ep.Stats)
			})

			// for the first row, parse the input
			// string into ion, then compare literally
			// with the first datum in the output
			var st ion.Symtab
			bytes := dst.Bytes()
			for i, expected := range tcs[i].expectedRows {
				if len(bytes) == 0 {
					t.Fatalf("couldn't read row #%d: not enough data", i)
				}

				row, rest, err := ion.ReadDatum(&st, bytes)
				if err != nil {
					t.Fatalf("couldn't read row #%d: %s", i, err)
				}
				bytes = rest

				want, err := ion.FromJSON(&st, json.NewDecoder(strings.NewReader(expected)))
				if err != nil {
					t.Fatalf("string #%d %q is not JSON: %s", i, expected, err)
				}

				if !ion.Equal(row, want) {
					t.Errorf("row #%d", i)
					t.Errorf("got : %#v", row)
					t.Errorf("want: %#v", want)

					t.Errorf("got JSON: %s", toJSON(&st, row))
					t.Errorf("want JSON: %s", toJSON(&st, want))
				}
			}
		})
	}
}

func BenchmarkPlan(b *testing.B) {
	env := &testenv{t: b}
	queries := []string{
		`select Make, (Ticket <> 1103341116 OR does_not_exist < 3) IS TRUE as e from parking limit 1`,
		`select Ticket, IssueTime, Make from parking
         where ViolationCode like '80.69A+'
         order by Make desc, IssueTime limit 15`,
	}

	for i := range queries {
		buf := []byte(queries[i])
		b.Run(fmt.Sprintf("Parse+Plan/case-%d", i), func(b *testing.B) {
			b.ReportAllocs()
			for j := 0; j < b.N; j++ {
				sel, err := partiql.Parse(buf)
				if err != nil {
					b.Fatal(err)
				}
				_, err = New(sel, env)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		sel, err := partiql.Parse(buf)
		if err != nil {
			b.Fatal(err)
		}
		tree, err := New(sel, env)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("Encode+Decode/case-%d", i), func(b *testing.B) {
			var buf ion.Buffer
			var st ion.Symtab
			b.ReportAllocs()
			for j := 0; j < b.N; j++ {
				buf.Reset()
				err := tree.Encode(&buf, &st)
				if err != nil {
					b.Fatal(err)
				}
				_, err = Decode(&st, buf.Bytes())
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func buf2json(st *ion.Symtab, buf *ion.Buffer) string {
	var otmp ion.Buffer
	st.Marshal(&otmp, true)
	otmp.UnsafeAppend(buf.Bytes())

	var out strings.Builder
	ion.ToJSON(&out, bufio.NewReader(bytes.NewReader(otmp.Bytes())))
	return out.String()
}

func testSerialize(t *testing.T, e expr.Node) {
	var obuf ion.Buffer
	var st ion.Symtab
	e.Encode(&obuf, &st)
	d, _, err := ion.ReadDatum(&st, obuf.Bytes())
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}

	res, err := expr.Decode(d)
	if err != nil {
		t.Helper()
		t.Logf("js: %s", buf2json(&st, &obuf))
		t.Fatal(err)
	}

	if !expr.Equivalent(e, res) {
		t.Errorf("input : %s", e)
		t.Errorf("output: %s", res)
	}
}

// funkyPipe is a pipe wrapper
// that randomizes read and write
// boundaries
type funkyPipe struct {
	io.ReadWriteCloser
}

func (f funkyPipe) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		c := rand.Intn(len(p)) + 1
		nn, err := f.ReadWriteCloser.Write(p[:c])
		n += nn
		if err != nil {
			return n, err
		}
		p = p[c:]
	}
	return n, nil
}

func (f funkyPipe) Read(p []byte) (int, error) {
	return f.ReadWriteCloser.Read(p[:1+rand.Intn(len(p))])
}

func testRemoteEquivalent(t *testing.T, tree *Tree,
	env *testenv, got []byte, wantstat *ExecStats) {
	local, remote := net.Pipe()

	var buf bytes.Buffer
	var remoteerr error
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		remoteerr = Serve(funkyPipe{remote}, env)
	}()

	c := Client{Pipe: funkyPipe{local}}
	ep := &ExecParams{
		Plan:    tree,
		Output:  &buf,
		Context: context.Background(),
		Runner:  env,
	}
	err := c.Exec(ep)
	if err != nil {
		t.Errorf("local error: %s", err)
	}
	if !bytes.Equal(buf.Bytes(), got) {
		t.Error("output not equivalent", len(buf.Bytes()), len(got))
	}
	err = c.Close()
	if err != nil {
		t.Errorf("Client.Close: %s", err)
	}
	wg.Wait()
	if remoteerr != nil {
		t.Errorf("remote error: %s", remoteerr)
	}
	if ep.Stats != *wantstat {
		t.Errorf("got stats %#v", &ep.Stats)
		t.Errorf("wanted stats %#v", wantstat)
	}
}

type splitEnv struct {
	Env
	geom *Geometry
}

func (e *splitEnv) ListTables(db string) ([]string, error) {
	tl, ok := e.Env.(TableLister)
	if !ok {
		return nil, fmt.Errorf("listing not supported")
	}
	return tl.ListTables(db)
}

func (e *splitEnv) Geometry() *Geometry {
	return e.geom
}

func testSplitEquivalent(t *testing.T, text string, e *testenv, expected []string, wantstat *ExecStats) {
	s, err := partiql.Parse([]byte(text))
	if err != nil {
		t.Fatal(err)
	}

	se := &splitEnv{
		Env: e,
		geom: &Geometry{
			Peers: []Transport{&LocalTransport{}, &LocalTransport{}},
		},
	}
	tree, err := NewSplit(s, se)
	if err != nil {
		t.Fatal(err)
	}
	var ib ion.Buffer
	var st ion.Symtab

	tree.ID = "my-random-tree-id"
	// Encode+Decode the plan, just to
	// be certain that the serialization process
	// is behavior-preserving
	err = tree.Encode(&ib, &st)
	if err != nil {
		t.Fatal(err)
	}
	newtree, err := Decode(&st, ib.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	// let's assume equal textual representation
	// means semantically equivalent:
	astr := tree.String()
	bstr := newtree.String()
	if astr != bstr {
		t.Errorf("old plan: %s", tree)
		t.Fatalf("new plan: %s", newtree)
	}
	if newtree.ID != tree.ID {
		t.Errorf("tree ID %s != %s", newtree, tree)
	}

	t.Logf("plan:\n%s", tree.String())

	tree = newtree
	st.Reset()
	var out bytes.Buffer
	ep := &ExecParams{
		Plan:   tree,
		Output: &out,
		Runner: e,
	}
	err = Exec(ep)
	if err != nil {
		t.Fatal(err)
	}
	if out.Len() == 0 && len(expected) != 0 {
		t.Fatal("no output, but non-zero number of output rows expected")
	}

	// for the first row, parse the input
	// string into ion, then compare literally
	// with the first datum in the output
	bytes := out.Bytes()
	for i, expected := range expected {
		if len(bytes) == 0 {
			t.Fatalf("couldn't read row #%d: not enough data", i)
		}

		row, rest, err := ion.ReadDatum(&st, bytes)
		if err != nil {
			t.Fatalf("couldn't read row #%d: %s", i, err)
		}
		bytes = rest

		want, err := ion.FromJSON(&st, json.NewDecoder(strings.NewReader(expected)))
		if err != nil {
			t.Fatalf("string #%d %q is not JSON: %s", i, expected, err)
		}

		if !ion.Equal(row, want) {
			t.Errorf("row #%d", i)
			t.Errorf("got : %#v", row)
			t.Errorf("want: %#v", want)

			t.Errorf("got JSON: %s", toJSON(&st, row))
			t.Errorf("want JSON: %s", toJSON(&st, want))
		}
	}
	// FIXME: we currently cannot deduplicate
	// inputs across union maps, so stats for
	// split queries are not expected to match the
	// original query
	if ep.Stats != *wantstat {
		t.Logf("got stats %#v", &ep.Stats)
		t.Logf("wanted stats %#v", wantstat)
	}
}

func testPlanSerialize(t *testing.T, tree *Tree) {
	var obuf ion.Buffer
	var st ion.Symtab

	str0 := tree.String()
	err := tree.Encode(&obuf, &st)
	if err != nil {
		t.Fatal(err)
	}
	tree2, err := Decode(&st, obuf.Bytes())
	if err != nil {
		t.Logf("json: %s", buf2json(&st, &obuf))
		t.Fatal(err)
	}
	str1 := tree2.String()
	if str0 != str1 {
		t.Errorf("input : %s", str0)
		t.Errorf("output: %s", str1)
	}
}

// test that server errors are correctly
// propogated into client errors
func TestServerError(t *testing.T) {
	remote, local := net.Pipe()
	defer local.Close()
	defer remote.Close()
	env := &testenv{t: t}

	var serverr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverr = Serve(remote, env)
	}()

	query := `select * from parking limit 1`
	s, err := partiql.Parse([]byte(query))
	if err != nil {
		t.Fatal(err)
	}
	tree, err := New(s, env)
	if err != nil {
		t.Fatal(err)
	}

	// now break the environment
	// shared with the server
	env.mustfail = "deliberate failure"

	cl := Client{Pipe: local}
	var out bytes.Buffer
	ep := &ExecParams{
		Plan:    tree,
		Output:  &out,
		Context: context.Background(),
	}
	err = cl.Exec(ep)
	if err == nil {
		t.Fatal("no failure message?")
	}
	if !strings.HasSuffix(err.Error(), env.mustfail) {
		t.Errorf("unexpected error %q", err)
	}
	if out.Len() != 0 {
		t.Errorf("accumulated buffered data (len=%d)", out.Len())
	}
	err = cl.Close()
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if serverr == nil {
		// server should produce the same error
		t.Fatal("no server error")
	}
}

type hangRunner struct{}

func (r hangRunner) Run(dst vm.QuerySink, src *Input, ep *ExecParams) error {
	<-ep.Context.Done()
	return ep.Context.Err()
}

type cancelOnRead struct {
	io.ReadWriteCloser
	cancel func()
}

func (c *cancelOnRead) Read(p []byte) (int, error) {
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	return c.ReadWriteCloser.Read(p)
}

func TestClientCancel(t *testing.T) {
	remote, local := net.Pipe()
	defer local.Close()
	defer remote.Close()
	env := &testenv{t: t}

	var serverr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// on the server side, use a bad runner
		// that will hang forever when Runner.Run() is called
		defer wg.Done()
		serverr = Serve(remote, hangRunner{})
	}()

	query := `select * from parking limit 1`
	s, err := partiql.Parse([]byte(query))
	if err != nil {
		t.Fatal(err)
	}
	tree, err := New(s, env)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	// we don't call cancel until the first
	// call to Read, so we are guaranteed that
	// we are in the read loop before the cancellation
	// happens:
	cl := Client{Pipe: &cancelOnRead{local, cancel}}
	ep := &ExecParams{
		Plan:    tree,
		Output:  &out,
		Context: ctx,
	}
	err = cl.Exec(ep)
	if err == nil {
		t.Fatal("no failure message?")
	}
	if !errors.Is(err, context.Canceled) {
		t.Error("error isn't context.Cancelled?")
	}
	err = cl.Close()
	if err != nil {
		t.Fatal(err)
	}
	// because we closed the pipe,
	// the server context should be canceled as well,
	// and so this shouldn't block indefinitely:
	wg.Wait()
	if !errors.Is(serverr, context.Canceled) {
		t.Fatalf("got error %T %[1]s", serverr)
	}
}

type testindexer map[string]Index

func (t testindexer) Index(e expr.Node) (Index, error) {
	switch e := e.(type) {
	case expr.String:
		return t[string(e)], nil
	case expr.Ident:
		return t[string(e)], nil
	}
	return nil, fmt.Errorf("unsupported table expr: %s", expr.ToString(e))
}

type testindex map[string][2]date.Time

func (t testindex) HasPartition(x string) bool { return false }

func (t testindex) TimeRange(p []string) (min, max date.Time, ok bool) {
	r, ok := t[p[0]]
	return r[0], r[1], ok
}

func dates(min, max string) [2]date.Time {
	dmin, ok := date.Parse([]byte(min))
	if !ok {
		panic("bad min date: " + min)
	}
	dmax, ok := date.Parse([]byte(max))
	if !ok {
		panic("bad max date: " + max)
	}
	return [2]date.Time{dmin, dmax}
}
