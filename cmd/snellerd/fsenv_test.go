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
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
	err := exec.Command("go", "build",
		"-o", "snellerd-test-binary", "-buildmode=exe", "-tags=vmfence,test", ".").Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

type testTenant struct {
	root *db.DirFS
	key  *blockfmt.Key
}

type dirResolver struct {
	*db.DirFS
}

func (d *dirResolver) Split(pattern string) (db.InputFS, string, error) {
	if !strings.HasPrefix(pattern, "file://") {
		return nil, "", fmt.Errorf("bad pattern %q", pattern)
	}
	pattern = strings.TrimPrefix(pattern, "file://")
	return d.DirFS, pattern, nil
}

func (t *testTenant) ID() string                { return "test-tenant" }
func (t *testTenant) Root() (db.InputFS, error) { return t.root, nil }
func (t *testTenant) Key() *blockfmt.Key        { return t.key }

func (t *testTenant) Split(pat string) (db.InputFS, string, error) {
	dr := dirResolver{t.root}
	return dr.Split(pat)
}

func randomKey() *blockfmt.Key {
	ret := new(blockfmt.Key)
	rand.Read(ret[:])
	return ret
}

func newTenant(root *db.DirFS) *testTenant {
	return &testTenant{
		root: root,
		key:  randomKey(),
	}
}

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
			{Pattern: "file://a-prefix/*.10n"},
		},
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

	b := db.Builder{
		Align:         4096,
		RangeMultiple: 10,
		Fallback: func(_ string) blockfmt.RowFormat {
			return blockfmt.UnsafeION()
		},
	}
	tt := newTenant(dfs)
	err = b.Sync(tt, "default", "*")
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
	if res.StatusCode != 200 {
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
		if res.StatusCode != 200 {
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
		if res.StatusCode != 200 {
			t.Fatalf("get tables: %s", res.Status)
		}
		var tables []string
		err = json.NewDecoder(res.Body).Decode(&tables)
		res.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(tables)
		want := []string{"parking", "parking2", "taxi"}
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
		if res.StatusCode != 200 {
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

	checkTiming := func(res *http.Response) {
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

	queries := []struct {
		input, db string
		output    string // regex
		partial   bool   // expect only a partial scan
	}{
		// get coverage of both empty db and default db
		{"SELECT COUNT(*) FROM default.parking", "", `{"count": 1023}`, false},
		{"SELECT COUNT(*) FROM parking", "default", `{"count": 1023}`, false},
		// check base case for taxi
		{"SELECT COUNT(*) FROM default.taxi", "", `{"count": 8560}`, false},
		// this WHERE is a no-op; everything satisfies it
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime >= `2009-01-01T00:35:23Z`", "", `{"count": 8560}`, false},
		// select all but the lowest
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime > `2009-01-01T00:35:23Z`", "", `{"count": 8559}`, false},
		// only the very first entries satisfies this:
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime <= `2009-01-01T00:35:23Z`", "", `{"count": 1}`, true},

		// ensure ORDER BY is accepted for cardinality=1 results
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime <= `2009-01-01T00:35:23Z` ORDER BY COUNT(*) DESC", "", `{"count": 1}`, true},

		// these two should be satisfied w/o scanning
		{"SELECT EARLIEST(tpep_pickup_datetime) FROM default.taxi", "", `{"min": "2009-01-01T00:35:23Z"}`, true},
		{"SELECT LATEST(tpep_pickup_datetime) FROM default.taxi", "", `{"max": "2009-01-31T23:55:00Z"}`, true},

		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime < `2009-01-01T00:35:23Z`", "", `{"count": 0}`, true},
		// about half of the entries satisfy this:
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime >= `2009-01-15T00:00:00Z`", "", `{"count": 4853}`, true},
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime < `2009-01-15T00:00:00Z`", "", `{"count": 3707}`, true},
		// similar to above; different date range
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime >= `2009-01-14T00:06:00Z`", "", `{"count": 5169}`, true},
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime < `2009-01-14T00:06:00Z`", "", `{"count": 3391}`, true},
		{
			// get coverage of the same table
			// being referenced more than once
			`WITH top_vendors AS (SELECT COUNT(*), VendorID FROM default.taxi GROUP BY VendorID ORDER BY COUNT(*) DESC)
SELECT ROUND(SUM(total_amount)) AS "sum" FROM default.taxi WHERE VendorID = (SELECT VendorID FROM top_vendors LIMIT 1)`,
			"",
			`{"sum": 76333}`, // rounded so that floating point noise doesn't break the test
			false,
		},
		{`SELECT COUNT(*) FROM TABLE_GLOB("[pt]a*")`, "default", `{"count": 10666}`, false},
		{`SELECT COUNT(*) FROM TABLE_GLOB("ta*") ++ TABLE_GLOB("pa*")`, "default", `{"count": 10666}`, false},
		{`SELECT * INTO foo.bar FROM default.taxi`, "", `{"table": "foo\.bar-.*"}`, false},
	}
	for i := range queries {
		r := rq.getQuery(queries[i].db, queries[i].input)
		res, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("status %s", res.Status)
		}
		var buf bytes.Buffer
		_, err = ion.ToJSON(&buf, bufio.NewReader(res.Body))
		res.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		m := regexp.MustCompile("^" + queries[i].output + "$")
		if !m.MatchString(strings.TrimSpace(buf.String())) {
			t.Errorf("got result %s", buf.String())
			t.Errorf("wanted %s", queries[i].output)
		}
		tablesize, err := strconv.ParseInt(res.Header.Get("X-Sneller-Total-Table-Bytes"), 0, 64)
		if err != nil {
			t.Errorf("getting table size: %s", err)
		}
		scannedsize, err := strconv.ParseInt(res.Header.Get("X-Sneller-Max-Scanned-Bytes"), 0, 64)
		if err != nil {
			t.Errorf("getting scanned bytes: %s", err)
		}
		t.Logf("scanned %d of %d", scannedsize, tablesize)
		if scannedsize%4096 != 0 {
			t.Errorf("scanned size %d not a multiple of the block size", scannedsize)
		}
		if scannedsize > tablesize {
			t.Errorf("scanned size %d > table size %d ?", scannedsize, tablesize)
		}
		// coarse check that sparse indexing actually did something:
		if (tablesize == 0 || scannedsize < tablesize) != queries[i].partial {
			t.Errorf("partial=%v, scanned=%d, all=%d", queries[i].partial, scannedsize, tablesize)
		}
		checkTiming(res)
		if i%4 == 3 {
			// occasionally establish new connections
			http.DefaultClient.CloseIdleConnections()
		}
	}

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
		checkTiming(res)
	}
}
