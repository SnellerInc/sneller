// Copyright (C) 2022 Sneller, Inc.
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/tenant"

	"golang.org/x/exp/slices"
)

func TestMain(m *testing.M) {
	// build the test binary launched with "stub" just once

	tags := "test"

	// vmfence is only available on Linux
	if runtime.GOOS == "linux" {
		tags += ",vmfence"
	}

	err := exec.Command("go", "build",
		"-o", "snellerd-test-binary", "-buildmode=exe", "-tags="+tags, ".").Run()

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to compile snellerd-test-binary: status %d", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

const testBlocksize = 4096

// create an environment
// with default.parking and default.taxi
// as db+table names, all populated in a tempdir
// with the appropriate indexes, etc.
func testdirEnviron(t *testing.T) db.Tenant {
	tmpdir := t.TempDir()

	for _, dir := range []string{
		filepath.Join(tmpdir, "a-prefix"),
		filepath.Join(tmpdir, "b-prefix"),
	} {
		err := os.MkdirAll(dir, 0750)
		if err != nil {
			t.Fatal(err)
		}
	}

	type link struct {
		src, dst string
	}
	links := []link{
		{"parking.10n", "a-prefix/parking.10n"},
		{"parking2.json", "a-prefix/parking2.json"},
		{"parking3.json", "a-prefix/parking3.json"},
		{"nyc-taxi-sorted.block", "b-prefix/nyc-taxi.block"},
	}
	for _, lnk := range links {
		newname := filepath.Join(tmpdir, lnk.dst)
		oldname, err := filepath.Abs("../../testdata/" + lnk.src)
		if err != nil {
			t.Fatal(err)
		}
		err = os.Symlink(oldname, newname)
		if err != nil {
			t.Fatal(err)
		}
	}
	dfs := db.NewDirFS(tmpdir)
	t.Cleanup(func() { dfs.Close() })
	dfs.Log = t.Logf

	err := db.WriteDefinition(dfs, "default", &db.Definition{
		Name: "parking",
		Inputs: []db.Input{
			{Pattern: "file://a-{prefix}/*.10n"},
		},
		Partitions: []db.Partition{{Field: "prefix"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.WriteDefinition(dfs, "default", &db.Definition{
		Name: "parking2",
		Inputs: []db.Input{
			{Pattern: "file://a-prefix/*.json", Format: "json"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.WriteDefinition(dfs, "default", &db.Definition{
		Name: "taxi",
		Inputs: []db.Input{
			{Pattern: "file://b-prefix/*.block"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.WriteDefinition(dfs, "default", &db.Definition{
		Name: "combined",
		Inputs: []db.Input{
			{Pattern: "file://a-prefix/{dataset}.json", Format: "json"},
			{Pattern: "file://b-prefix/{dataset}.block"},
		},
		Partitions: []db.Partition{{
			Field: "dataset",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	c := db.Config{
		Align:         testBlocksize,
		RangeMultiple: 10,
		Fallback: func(_ string) blockfmt.RowFormat {
			return blockfmt.UnsafeION()
		},
	}
	tt := db.NewLocalTenant(dfs)
	err = c.Sync(tt, "default", "*")
	if err != nil {
		t.Fatal(err)
	}
	return tt
}

func listen(t *testing.T) net.Listener {
	sock, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sock.Close() })
	return sock
}

func testFiles(t *testing.T) {
	now, err := os.ReadDir("/proc/self/fd")
	if err == nil {
		count := len(now) - 1
		t.Cleanup(func() {
			http.DefaultClient.CloseIdleConnections()
			done, err := os.ReadDir("/proc/self/fd")
			if err != nil {
				t.Error(err)
			}
			if len(done)-1 > count {
				t.Errorf("file descriptor leak: started with %d, now have %d", count, len(done)-1)
			}
		})
	} else {
		t.Log("(can't do file descriptor tracking)")
	}
}

func TestQueryError(t *testing.T) {
	tt := testdirEnviron(t)
	peersock0, peersock1 := listen(t), listen(t)
	_ = peersock1
	s := server{
		logger:    testlogger(t),
		sandbox:   tenant.CanSandbox(),
		cachedir:  t.TempDir(),
		cgroot:    os.Getenv("CGROOT"),
		tenantcmd: []string{"./snellerd-test-binary", "worker"},
		splitSize: 16 * 1024,
		peers: makePeers(t,
			peersock0.Addr().(*net.TCPAddr),
			peersock1.Addr().(*net.TCPAddr),
			// add a bad peer address that should be
			// filtered out on peer resolution:
			&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 54423},
		),
		auth: testAuth{tt},
	}
	httpsock := listen(t)
	// this second peer is just here
	// to allow for the query to actually
	// be split
	peer := server{
		logger:    testlogger(t),
		sandbox:   s.sandbox,
		cachedir:  t.TempDir(),
		tenantcmd: s.tenantcmd,
		splitSize: s.splitSize,
		peers:     makePeers(t, peersock0.Addr().(*net.TCPAddr), peersock1.Addr().(*net.TCPAddr)),
	}
	httpsock2 := listen(t)

	// start the servers and wait
	// for them to start serving;
	// this makes '-race' happy with
	// the ordering of Serve() and Close()
	// across goroutines
	var wg sync.WaitGroup
	wg.Add(2)
	s.aboutToServe = (&wg).Done
	peer.aboutToServe = (&wg).Done
	go s.Serve(httpsock, peersock0)
	go peer.Serve(httpsock2, peersock1)
	wg.Wait()

	defer s.Close()
	defer peer.Close()

	rq := &requester{
		t:    t,
		host: "http://" + httpsock.Addr().String(),
	}

	// create a sub-query that interpolates
	// more than 10,000 results; this should
	// result in a runtime error
	query := `SELECT
   (SELECT DISTINCT COALESCE(Ticket,Issue.Tick,tpep_pickup_datetime),
                    COALESCE(Ticket,Issue.Tick,tpep_dropoff_datetime)
     FROM default.taxi ++ default.parking2 ++ default.parking)
   AS list`
	r := rq.getQuery("", query)
	res, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	buf, _ := io.ReadAll(res.Body)
	// query should begin successfully:
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status code %d %s", res.StatusCode, buf)
	}
	var st ion.Symtab
	buf, err = st.Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}
	// expect query_error::{error_message: "..."}
	sym, body, _, err := ion.ReadAnnotation(buf)
	if err != nil {
		t.Fatal(err)
	}
	if st.Get(sym) != "query_error" {
		t.Errorf("annotation is %q?", st.Get(sym))
	}
	if ion.TypeOf(body) != ion.StructType {
		t.Fatalf("type of query_error annotation is %s", ion.TypeOf(body))
	}
	body, _ = ion.Contents(body)
	sym, body, err = ion.ReadLabel(body)
	if err != nil {
		t.Fatal(err)
	}
	if st.Get(sym) != "error_message" {
		t.Errorf("first field of query_error::{...} is %q", st.Get(sym))
	}
	msg, _, err := ion.ReadString(body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(msg, "subreplacement exceeds limit") {
		t.Errorf("unexpected error message %q", msg)
	}
	t.Logf("error message: %s", msg)
}

// test the server running on a tmpfs that
// has been populated with some test tables
func TestSimpleFS(t *testing.T) {
	tt := testdirEnviron(t)
	peersock0, peersock1 := listen(t), listen(t)
	_ = peersock1
	s := server{
		logger:    testlogger(t),
		sandbox:   tenant.CanSandbox(),
		cachedir:  t.TempDir(),
		cgroot:    os.Getenv("CGROOT"),
		tenantcmd: []string{"./snellerd-test-binary", "worker"},
		splitSize: 16 * 1024,
		peers: makePeers(t,
			peersock0.Addr().(*net.TCPAddr),
			peersock1.Addr().(*net.TCPAddr),
			// add a bad peer address that should be
			// filtered out on peer resolution:
			&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 54423},
		),
		auth: testAuth{tt},
	}
	httpsock := listen(t)
	// this second peer is just here
	// to allow for the query to actually
	// be split
	peer := server{
		logger:    testlogger(t),
		sandbox:   s.sandbox,
		cachedir:  t.TempDir(),
		tenantcmd: s.tenantcmd,
		splitSize: s.splitSize,
		peers:     makePeers(t, peersock0.Addr().(*net.TCPAddr), peersock1.Addr().(*net.TCPAddr)),
	}
	httpsock2 := listen(t)

	// start the servers and wait
	// for them to start serving;
	// this makes '-race' happy with
	// the ordering of Serve() and Close()
	// across goroutines
	var wg sync.WaitGroup
	wg.Add(2)
	s.aboutToServe = (&wg).Done
	peer.aboutToServe = (&wg).Done
	go s.Serve(httpsock, peersock0)
	go peer.Serve(httpsock2, peersock1)
	wg.Wait()

	defer s.Close()
	defer peer.Close()

	rq := &requester{
		t:    t,
		host: "http://" + httpsock.Addr().String(),
	}
	{
		req := rq.getDBs()
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("get /databases: %s", res.Status)
		}
		var dbs []database
		err = json.NewDecoder(res.Body).Decode(&dbs)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if len(dbs) != 1 {
			t.Fatalf("got dbs %v", dbs)
		}
		if dbs[0].Name != "default" {
			t.Fatalf("dbs[0] = %q", dbs[0])
		}
	}
	{
		// test that listing tables works
		req := rq.getTables("default")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("get tables: %s", res.Status)
		}
		var tables []string
		err = json.NewDecoder(res.Body).Decode(&tables)
		res.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(tables)
		want := []string{"combined", "parking", "parking2", "taxi"}
		if !slices.Equal(tables, want) {
			t.Fatalf("got tables: %v", tables)
		}
	}
	{
		// test that listing index contents work
		req := rq.getInputs("default", "parking")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("get index contents: %s", res.Status)
		}
		var ret struct {
			Path     string `json:"path"`
			ETag     string `json:"etag"`
			Accepted bool   `json:"accepted"`
		}
		d := json.NewDecoder(res.Body)
		for err = d.Decode(&ret); !errors.Is(err, io.EOF); err = d.Decode(&ret) {
			if !ret.Accepted {
				t.Errorf("file %s not accepted?", ret.Path)
			}
			ret.Path = ""
			ret.ETag = ""
			ret.Accepted = false
		}
		if !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
	}

	checkTiming := func(t *testing.T, res *http.Response) {
		t.Helper()
		timings := res.Trailer.Get("Server-Timing")
		if timings == "" {
			t.Error("no server timings")
			return
		}
		fields := strings.Split(timings, ",")
		for i := range fields {
			field := strings.TrimSpace(fields[i])
			keyvalues := strings.Split(field, ";")
			if keyvalues[0] == "error" {
				t.Error("query encountered an error")
			}
			switch keyvalues[0] {
			case "exec", "miss", "hit", "scanned":
			default:
				t.Errorf("unrecognized Server-Timing response %v", keyvalues)
			}
		}
	}

	checkAnnotation := func(t *testing.T, body []byte, maxscan int64) {
		var d, final ion.Datum
		dec := ion.NewDecoder(bytes.NewReader(body), 64*1024)
		dec.ExtraAnnotations = map[string]any{
			"final_status": &final,
		}
		for {
			err := dec.Decode(&d)
			if err == nil {
				continue
			}
			if !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if final.IsEmpty() {
				t.Fatal("missing final_status trailer")
			}
			if !final.Field("error").IsEmpty() {
				str, _ := final.Field("error").String()
				t.Fatalf("query error: %s", str)
			}
			scanned, _ := final.Field("scanned").Uint()
			if maxscan > 0 && scanned == 0 {
				t.Fatalf("scanned = 0; maxscan = %d", maxscan)
			} else if int64(scanned) > maxscan {
				t.Fatalf("maxscan = %d, scanned = %d", maxscan, scanned)
			}
			t.Logf("scanned = %d", scanned)
			break
		}
	}

	queries := []struct {
		input, db string
		output    string // exact output, or regular expression
		partial   bool   // expect only a partial scan
		rx        bool   // use regular expression
		status    int    // if non-zero, expected HTTP status code
	}{
		// get coverage of both empty db and default db
		{input: "SELECT COUNT(*) FROM default.parking", output: `{"count": 1023}`},
		// group by partition coverage:
		{input: "SELECT COUNT(*), prefix FROM default.parking GROUP BY prefix", output: `{"count": 1023, "prefix": "prefix"}`},
		{input: "SELECT COUNT(*) FROM parking", db: "default", output: `{"count": 1023}`},
		// check base case for taxi
		{input: "SELECT COUNT(*) FROM default.taxi", output: `{"count": 8560}`},
		// this WHERE is a no-op; everything satisfies it
		{input: "SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime >= `2009-01-01T00:35:23Z`", output: `{"count": 8560}`},
		// select all but the lowest
		{input: "SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime > `2009-01-01T00:35:23Z`", output: `{"count": 8559}`},
		// only the very first entries satisfies this:
		{input: "SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime <= `2009-01-01T00:35:23Z`", output: `{"count": 1}`, partial: true},

		// we don't really care about the results from these queries;
		// we just need to get coverage of early errors from io.Writers
		// in vm.TeeWriter
		{input: "SELECT * FROM default.taxi LIMIT 3", output: `{(.*?\n{){2}.*`, rx: true},
		{input: "SELECT * FROM default.taxi LIMIT 5", output: `{(.*?\n{){4}.*`, rx: true},
		{input: "SELECT * FROM default.taxi LIMIT 7", output: `{(.*?\n{){6}.*`, rx: true},

		// ensure ORDER BY is accepted for cardinality=1 results
		{input: "SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime <= `2009-01-01T00:35:23Z` ORDER BY COUNT(*) DESC", output: `{"count": 1}`, partial: true},

		// these two should be satisfied w/o scanning
		{input: "SELECT EARLIEST(tpep_pickup_datetime) FROM default.taxi", output: `{"min": "2009-01-01T00:35:23Z"}`, partial: true},
		{input: "SELECT LATEST(tpep_pickup_datetime) FROM default.taxi", output: `{"max": "2009-01-31T23:55:00Z"}`, partial: true},

		{input: "SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime < `2009-01-01T00:35:23Z`", output: `{"count": 0}`, partial: true},
		// about half of the entries satisfy this:
		{input: "SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime >= `2009-01-15T00:00:00Z`", output: `{"count": 4853}`, partial: true},
		{input: "SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime < `2009-01-15T00:00:00Z`", output: `{"count": 3707}`, partial: true},
		// similar to above; different date range
		{input: "SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime >= `2009-01-14T00:06:00Z`", output: `{"count": 5169}`, partial: true},
		{input: "SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime < `2009-01-14T00:06:00Z`", output: `{"count": 3391}`, partial: true},
		{
			input:   "SELECT (SELECT COUNT(tpep_pickup_datetime) FROM default.taxi WHERE tpep_pickup_datetime < `2009-01-14T00:06:00Z`) AS count0, (SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime < `2009-01-14T00:06:00Z`) AS count1",
			output:  `{"count0": 3391, "count1": 3391}`,
			partial: true,
		},
		{input: "SELECT COUNT(*), VendorID FROM default.taxi GROUP BY VendorID ORDER BY SUM(trip_distance) DESC", output: "{\"count\": 7353, \"VendorID\": \"VTS\"}\n{\"count\": 1055, \"VendorID\": \"CMT\"}\n{\"count\": 152, \"VendorID\": \"DDS\"}"},
		{input: "SELECT COUNT(DISTINCT RPState) from default.parking", output: `{"count": 25}`},

		// don't care much about the result here; this just
		// exercises the vm scratch save+restore code
		{input: "SELECT COUNT(*), tm FROM default.taxi GROUP BY DATE_TRUNC(DAY, tpep_pickup_datetime) AS tm", output: ".*", rx: true},
		{
			// get coverage of the same table
			// being referenced more than once
			input: `WITH top_vendors AS (SELECT COUNT(*), VendorID FROM default.taxi GROUP BY VendorID ORDER BY COUNT(*) DESC)
SELECT ROUND(SUM(total_amount)) AS "sum" FROM default.taxi WHERE VendorID = (SELECT VendorID FROM top_vendors LIMIT 1)`,
			output: `{"sum": 76333}`, // rounded so that floating point noise doesn't break the test
		},
		{input: `SELECT COUNT(*) FROM TABLE_GLOB("[pt]a*")`, db: "default", output: `{"count": 10666}`},
		{input: `SELECT COUNT(*) FROM TABLE_GLOB("ta*") ++ TABLE_GLOB("pa*")`, db: "default", output: `{"count": 10666}`},
		{input: `SELECT * INTO foo.bar FROM default.taxi`, output: `{"table": "foo\..*`, rx: true},
		{input: "SELECT COUNT(*) from default.combined WHERE dataset = 'parking2'", output: `{"count": 1023}`},
		{input: "SELECT COUNT(*) from default.combined WHERE dataset = 'parking3'", output: `{"count": 60}`},
		{input: "SELECT COUNT(*) from default.combined WHERE dataset = 'nyc-taxi'", output: `{"count": 8560}`},
		// Note: 'default1' is not a valid path, an indexer returns error during
		//       parsing the FROM part.
		{input: "SELECT * FROM default1.taxi", status: http.StatusNotFound},
	}
	var subwg sync.WaitGroup
	subwg.Add(len(queries))
	for i := range queries {
		q := &queries[i]
		name := fmt.Sprintf("query%d", i)
		go func() {
			defer subwg.Done()
			t.Run(name, func(t *testing.T) {
				rq := &requester{
					t:    t,
					host: "http://" + httpsock.Addr().String(),
				}
				r := rq.getQuery(q.db, q.input)
				res, err := http.DefaultClient.Do(r)
				if err != nil {
					t.Fatal(err)
				}
				want := http.StatusOK
				if q.status != 0 {
					want = q.status
				}
				if res.StatusCode != want {
					t.Fatalf("got status code %d; wanted %d", res.StatusCode, want)
				}
				if res.StatusCode != http.StatusOK {
					// don't perform any more checks, query failed
					return
				}

				var buf, body bytes.Buffer
				_, err = ion.ToJSON(&buf, bufio.NewReader(io.TeeReader(res.Body, &body)))
				res.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				got := strings.TrimSpace(buf.String())
				if q.rx {
					m := regexp.MustCompilePOSIX("^" + q.output + "$")
					if !m.MatchString(got) {
						t.Errorf("got result %s", got)
						t.Errorf("wanted %s", m.String())
					}
				} else if got != q.output {
					t.Errorf("got result %s", got)
					t.Errorf("wanted %s", q.output)
				}
				scannedsize, err := strconv.ParseInt(res.Header.Get("X-Sneller-Max-Scanned-Bytes"), 0, 64)
				if err != nil {
					t.Errorf("getting scanned bytes: %s", err)
				}
				t.Logf("max scan %d bytes", scannedsize)
				if scannedsize%testBlocksize != 0 {
					t.Errorf("scanned size %d not a multiple of the block size", scannedsize)
				}
				checkAnnotation(t, body.Bytes(), scannedsize)
				checkTiming(t, res)
			})
		}()
	}
	subwg.Wait()

	// get coverage of JSON responses
	jsqueries := []struct {
		query, result string
	}{
		{
			query:  `SELECT Location FROM default.parking WHERE Route = '2A75' AND IssueTime = 945`,
			result: `[{"Location": "721 S WESTLAKE"}]`,
		},
		{
			query:  `SELECT Ticket FROM default.parking WHERE Route = '2A75' AND IssueTime <= 1100`,
			result: `[{"Ticket": 1106506402},{"Ticket": 1106506413},{"Ticket": 1106506424}]`,
		},
	}
	for i := range jsqueries {
		r := rq.getQueryJSON("", jsqueries[i].query)
		res, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("status %s", res.Status)
		}
		got, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != jsqueries[i].result {
			t.Errorf("got %q, want %q", got, jsqueries[i].result)
		}
		checkTiming(t, res)
	}
}
