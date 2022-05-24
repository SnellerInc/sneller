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
	"time"

	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/tenant"
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
	newname := filepath.Join(tmpdir, "a-prefix/parking.10n")
	oldname, err := filepath.Abs("../../testdata/parking.10n")
	if err != nil {
		t.Fatal(err)
	}
	err = os.Symlink(oldname, newname)
	if err != nil {
		t.Fatal(err)
	}
	newname = filepath.Join(tmpdir, "b-prefix/nyc-taxi.block")
	oldname, err = filepath.Abs("../../testdata/nyc-taxi.block")
	if err != nil {
		t.Fatal(err)
	}
	err = os.Symlink(oldname, newname)
	if err != nil {
		t.Fatal(err)
	}

	dfs := db.NewDirFS(tmpdir)
	t.Cleanup(func() { dfs.Close() })
	dfs.Log = t.Logf

	err = db.WriteDefinition(dfs, "default", &db.Definition{
		Name: "parking",
		Inputs: []db.Input{
			{Pattern: "file://a-prefix/*.10n"},
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
		Align:         2048,
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
		peers:     makePeers(t, peersock0.Addr().(*net.TCPAddr), peersock1.Addr().(*net.TCPAddr)),
		auth:      testAuth{tt},
	}
	err := s.peers.Start(time.Second, t.Logf)
	if err != nil {
		t.Fatal(err)
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
	err = peer.peers.Start(time.Second, t.Logf)
	if err != nil {
		t.Fatal(err)
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
		if len(tables) != 2 || tables[0] != "parking" || tables[1] != "taxi" {
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
	}{
		// get coverage of both empty db and default db
		{"SELECT COUNT(*) FROM default.parking", "", `{"count": 1023}`},
		{"SELECT COUNT(*) FROM parking", "default", `{"count": 1023}`},
		// check base case for taxi
		{"SELECT COUNT(*) FROM default.taxi", "", `{"count": 8560}`},
		// this WHERE is a no-op; everything satisfies it
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime >= `2009-01-01T00:35:23Z`", "", `{"count": 8560}`},
		// select all but the lowest
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime > `2009-01-01T00:35:23Z`", "", `{"count": 8559}`},
		// only the very first entries satisfies this:
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime <= `2009-01-01T00:35:23Z`", "", `{"count": 1}`},

		// ensure ORDER BY is accepted for cardinality=1 results
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime <= `2009-01-01T00:35:23Z` ORDER BY COUNT(*) DESC", "", `{"count": 1}`},

		// these two should be satisfied w/o scanning
		{"SELECT EARLIEST(tpep_pickup_datetime) FROM default.taxi", "", `{"min": "2009-01-01T00:35:23Z"}`},
		{"SELECT LATEST(tpep_pickup_datetime) FROM default.taxi", "", `{"max": "2009-01-31T23:55:00Z"}`},

		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime < `2009-01-01T00:35:23Z`", "", `{"count": 0}`},
		// about half of the entries satisfy this:
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime >= `2009-01-15T00:00:00Z`", "", `{"count": 4853}`},
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime < `2009-01-15T00:00:00Z`", "", `{"count": 3707}`},
		// similar to above; different date range
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime >= `2009-01-14T00:06:00Z`", "", `{"count": 5169}`},
		{"SELECT COUNT(*) FROM default.taxi WHERE tpep_pickup_datetime < `2009-01-14T00:06:00Z`", "", `{"count": 3391}`},
		{
			// get coverage of the same table
			// being referenced more than once
			`WITH top_vendors AS (SELECT COUNT(*), VendorID FROM default.taxi GROUP BY VendorID ORDER BY COUNT(*) DESC)
SELECT SUM(total_amount) FROM default.taxi WHERE VendorID = (SELECT VendorID FROM top_vendors LIMIT 1)`,
			"",
			`{"sum": 76333.22931289673}`,
		},
		{`SELECT COUNT(*) FROM TABLE_GLOB("[pt]a*")`, "default", `{"count": 9583}`},
		{`SELECT COUNT(*) FROM TABLE_GLOB("ta*") ++ TABLE_GLOB("pa*")`, "default", `{"count": 9583}`},
		{`SELECT * INTO foo.bar FROM default.taxi`, "", `{"table": "foo\.bar-.*"}`},
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
		if scannedsize%2048 != 0 {
			t.Errorf("scanned size %d not a multiple of the block size", scannedsize)
		}
		if scannedsize > tablesize {
			t.Errorf("scanned size %d > table size %d ?", scannedsize, tablesize)
		}
		checkTiming(res)
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
