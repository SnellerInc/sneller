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

package tenant

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/cgroup"
	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/tnproto"
	"github.com/SnellerInc/sneller/usock"
	"github.com/SnellerInc/sneller/vm"
)

func randpair() (id tnproto.ID, key tnproto.Key) {
	rand.Read(id[:])
	rand.Read(key[:])
	return
}

type stubenv struct{}

type stubHandle string

func (s stubHandle) Open(_ context.Context) (vm.Table, error) {
	return nil, fmt.Errorf("shouldn't have opened stubenv locally!")
}

func (s stubHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("filename"))
	dst.WriteString(string(s))
	dst.EndStruct()
	return nil
}

type badHandle struct{}

func (b badHandle) Open(_ context.Context) (vm.Table, error) {
	return nil, fmt.Errorf("shouldn't have opened badHandle")
}

func (b badHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.WriteNull()
	return nil
}

type repeatHandle struct {
	count int
	file  string
}

func (r *repeatHandle) Open(_ context.Context) (vm.Table, error) {
	return nil, fmt.Errorf("shouldn't have opened repeatHandle")
}

func (r *repeatHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("repeat"))
	dst.WriteInt(int64(r.count))
	dst.BeginField(st.Intern("filename"))
	dst.WriteString(r.file)
	dst.EndStruct()
	return nil
}

type hangHandle struct {
	file string
}

func (h *hangHandle) Open(_ context.Context) (vm.Table, error) {
	panic("hangHandle.Open in parent")
}

func (h *hangHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("filename"))
	dst.WriteString(h.file)
	dst.BeginField(st.Intern("hang"))
	dst.WriteBool(true)
	dst.EndStruct()
	return nil
}

func (s stubenv) Stat(tbl expr.Node, _ *plan.Hints) (plan.TableHandle, error) {
	if b, ok := tbl.(*expr.Builtin); ok {
		switch b.Text {
		case "REPEAT":
			return &repeatHandle{count: int(b.Args[0].(expr.Integer)), file: string(b.Args[1].(expr.String))}, nil
		case "HANG":
			return &hangHandle{file: string(b.Args[0].(expr.String))}, nil
		default:
			return badHandle{}, nil
		}
	}
	// confirm that the file exists,
	// but otherwise do nothing
	fn, ok := tbl.(expr.String)
	if !ok {
		return badHandle{}, nil
	}
	_, err := os.Stat(string(fn))
	if err != nil {
		return nil, err
	}
	return stubHandle(fn), nil
}

func mkplan(t *testing.T, str string) *plan.Tree {
	s, err := partiql.Parse([]byte(str))
	if err != nil {
		t.Fatal(err)
	}
	tree, err := plan.New(s, stubenv{})
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

func fsize(fname string) int64 {
	f, err := os.Stat(fname)
	if err != nil {
		panic(err)
	}
	return f.Size()
}

var cgroot cgroup.Dir

func TestMain(m *testing.M) {
	// build the test binary launched with "stub" just once
	err := exec.Command("go", "build",
		"-o", "test-stub", "-buildmode=exe", "stub.go").Run()

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to compile test-stub: %s", err)
		os.Exit(1)
	}
	// allow testing w/ cgroups
	if c := os.Getenv("CGROOT"); c != "" {
		cgroot = cgroup.Dir(c)
	}
	os.Exit(m.Run())
}

func TestExec(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("this test will not work on windows")
	}
	// add a hook here so we can count
	// the number of times the eviction
	// hook has run; it should run exactly
	// once per input test case
	oldusage := usage
	evictcount := int32(0)
	usage = func(dir string) (int64, int64) {
		atomic.AddInt32(&evictcount, 1)
		return 0, 1000
	}
	t.Cleanup(func() {
		usage = oldusage
	})

	query := `SELECT * FROM '../testdata/parking.10n' LIMIT 1`
	l, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("listener: %d", usock.Fd(l))
	id, key := randpair()
	var logbuf bytes.Buffer

	opts := []Option{
		WithGCInterval(time.Hour),
		WithLogger(log.New(&logbuf, "manager-log: ", 0)),
		WithRemote(l),
	}
	// try to do delegated cgroup trickery
	if !cgroot.IsZero() {
		t.Logf("using cgroup %s", cgroot)
		opts = append(opts, WithCgroup(func(id tnproto.ID) cgroup.Dir {
			return cgroot.Sub(id.String())
		}),
			// pass the desired cgroup in the environment
			// so that the stub can test it is in the right place
			WithTenantEnv(func(cache string, id tnproto.ID) []string {
				base := DefaultEnv(cache, id)
				return append(base, fmt.Sprintf("WANT_CGROUP=%s", string(cgroot.Sub(id.String()))))
			}))
	}
	m := NewManager([]string{"./test-stub", "worker"}, opts...)
	// if bwrap(1) is installed,
	// test that sandboxing works
	m.Sandbox = CanSandbox()
	m.CacheDir = t.TempDir()

	nfds := func() int {
		dirents, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Helper()
			t.Fatal(err)
		}
		return len(dirents)
	}

	start := nfds()
	t.Cleanup(func() {
		now := nfds()
		if now > start {
			// NOTE: due to os.File.Close not immediately
			// closing files, this can very occasionally fail...
			t.Logf("open file descriptors: %v", openfds(t))
		}
	})

	t.Logf("start has these fds open: %v", openfds(t))
	here, there := socketPair(t)

	step2 := nfds()
	if step2 != start+2 {
		t.Errorf("fd leak: have %d file descriptors open; expected %d", step2, start+2)
	}

	rc, err := m.Do(id, key, mkplan(t, query), tnproto.OutputRaw, here)
	here.Close()
	if err != nil {
		t.Fatal(err)
	}
	var js bytes.Buffer
	rd := bufio.NewReader(there)
	_, err = ion.ToJSON(&js, rd)
	if err != nil {
		t.Errorf("reading response: %s", err)
	}

	var stats plan.ExecStats
	err = Check(rc, &stats)
	if err != nil {
		t.Fatalf("query error: %s", err)
	}
	there.Close()
	// there should be one eviction check
	// from when the goroutine was launched,
	// and then one for the query (although
	// this may change if go test ./... ends up
	// causing the evictions to be coalesced)
	if c := atomic.LoadInt32(&evictcount); c != 2 {
		t.Logf("got %d evictions", c)
	}

	// test a query that should yield
	// an immediate error

	here, there = socketPair(t)
	query = `SELECT * FROM '/dev/null' LIMIT 1` // 'dev/null' forces the stub process to return an error
	rc, err = m.Do(id, key, mkplan(t, query), tnproto.OutputRaw, here)
	if err == nil {
		t.Fatal("expected immediate error for query...?")
	}
	if rc != nil {
		t.Fatal("expected rc == nil when encounting an immediate error")
	}
	here.Close()
	there.Close()
	// we don't want just any error;
	// we want one that indicates the
	// tenant rejected the query
	rem := &tnproto.RemoteError{}
	if !errors.As(err, &rem) {
		t.Errorf("type of error returned is %T?", err)
	}
	t.Logf("deliberate error: %s", err)

	stopped := make(chan struct{})
	go func() {
		err := m.Serve()
		// expect a clean shutdown
		if err != nil {
			panic(err)
		}
		close(stopped)
	}()

	var (
		parkingSize = fsize("../testdata/parking.10n")
		nycSize     = fsize("../testdata/nyc-taxi.block")
	)
	// each of these subqueries is executed
	// as if the input table was concatenated
	// with itself four times, and we have
	// the "mapping" portions of the sub-query
	// execute by looping back into the Manager
	// via the tcp connection we bound above
	subqueries := []struct {
		query string
		want  []string
		count int
		scan  int64 // expected # of bytes to scan, if non-zero
	}{
		{
			query: `SELECT COUNT(*) FROM '../testdata/parking.10n'`,
			want:  []string{`{"count": 4092}`},
			scan:  parkingSize * 4,
		},
		{
			query: `SELECT COUNT(Make) FROM '../testdata/parking.10n'`,
			want:  []string{`{"count": 4076}`},
			scan:  parkingSize * 4,
		},
		{
			query: `SELECT MAX(Ticket) FROM '../testdata/parking.10n'`,
			want:  []string{`{"max": 4272473892}`},
			scan:  parkingSize * 4,
		},
		{
			query: `select MAX(Ticket + 1) from '../testdata/parking.10n'`,
			want:  []string{`{"max": 4272473893}`},
			scan:  parkingSize * 4,
		},
		{
			query: `select avg(fare_amount), VendorID from '../testdata/nyc-taxi.block' group by VendorID order by avg(fare_amount)`,
			want: []string{
				`{"VendorID": "VTS", "avg": 9.435699641166872}`,
				`{"VendorID": "CMT", "avg": 9.685402771563982}`,
				`{"VendorID": "DDS", "avg": 9.942763085526316}`,
			},
			scan: nycSize * 4,
		},
		{
			// test SELECT DISTINCT on column with known cardinality
			query: `select distinct Color from '../testdata/parking.10n' order by Color`,
			want: []string{
				`{"Color": "BG"}`, `{"Color": "BK"}`, `{"Color": "BL"}`, `{"Color": "BN"}`,
				`{"Color": "BR"}`, `{"Color": "BU"}`, `{"Color": "GN"}`, `{"Color": "GO"}`,
				`{"Color": "GR"}`, `{"Color": "GY"}`, `{"Color": "MA"}`, `{"Color": "MR"}`,
				`{"Color": "OR"}`, `{"Color": "OT"}`, `{"Color": "PR"}`, `{"Color": "RD"}`,
				`{"Color": "RE"}`, `{"Color": "SI"}`, `{"Color": "SL"}`, `{"Color": "TA"}`,
				`{"Color": "TN"}`, `{"Color": "WH"}`, `{"Color": "WT"}`, `{"Color": "YE"}`,
			},
			scan: parkingSize * 4,
		},
		{
			query: `select sum(total_amount)-sum(fare_amount) as diff, payment_type from '../testdata/nyc-taxi.block' group by payment_type order by diff desc`,
			want: []string{
				`{"diff": 19975.03998680011, "payment_type": "Credit"}`,
				`{"diff": 9900.999768399954, "payment_type": "CASH"}`,
				`{"diff": 372.4000076, "payment_type": "CREDIT"}`,
				`{"diff": 236.5999759999977, "payment_type": "Cash"}`,
				`{"diff": 0, "payment_type": "No Charge"}`,
				`{"diff": 0, "payment_type": "Dispute"}`,
			},
			scan: nycSize * 4,
		},
		{
			// test ORDER BY clause with LIMIT
			query: `select distinct Ticket from '../testdata/parking.10n' order by Ticket limit 4`,
			want: []string{
				`{"Ticket": 1103341116}`,
				`{"Ticket": 1103700150}`,
				`{"Ticket": 1104803000}`,
				`{"Ticket": 1104820732}`,
			},
			scan: parkingSize * 4,
		},
		{
			query: `select * from '../testdata/parking.10n' limit 6`,
			// we do not specify the row contents
			// because the contents of a LIMIT expression
			// are under-specified without an explicit ORDER BY
			count: 6,
		},
		{
			// this is a bit funky because it should cause
			// four consecutive failed cache fills;
			// each access locks the cache entry associated
			// with this data, scans a few records,
			// then aborts early due to the LIMIT
			query: `select * from REPEAT(4, '../testdata/nyc-taxi.block') limit 6`,
			count: 6,
		},
		{
			// this should only cause 1 fill
			// because there is no LIMIT
			query: `select count(*) from REPEAT(10, '../testdata/nyc-taxi.block')`,
			want: []string{
				`{"count": 342400}`,
			},
			scan: nycSize * 10 * 4,
		},
	}
	curfds := nfds()
	cachefills := atomic.LoadInt32(&evictcount)
	for i := range subqueries {
		t.Run(fmt.Sprintf("split-case-%d", i), func(t *testing.T) {
			count := subqueries[i].count
			if count < len(subqueries[i].want) {
				count = len(subqueries[i].want)
			}
			testEqual(t, subqueries[i].query, m, id, key, subqueries[i].want, count, subqueries[i].scan)
		})
		now := nfds()
		if curfds != now {
			t.Errorf("after sub-test: now %d file descriptors open", now)
		}
	}
	if f := atomic.LoadInt32(&evictcount) - cachefills; f != 3 {
		// if /tmp is being used for other tests
		// via 'go test ./...' then the number of
		// evicts/fills becomes unreliable, so we
		// can't make this a hard error
		t.Logf("expected 6 cache fills; found %d (%d - %d)", f, atomic.LoadInt32(&evictcount), cachefills)
	}

	// test cancelation via closing of the returned status socket
	t.Run("cancel", func(t *testing.T) {
		testCancel(t, m)
	})

	// test that using the wrong key causes an
	// error to be returned
	t.Run("authorize", func(t *testing.T) {
		tree := mkplan(t, query)
		_, badkey := randpair()
		_, err := m.Do(id, badkey, tree, tnproto.OutputRaw, there)
		if err == nil {
			t.Error("expected error, got none")
		} else if !strings.Contains(err.Error(), "key mismatch") {
			t.Errorf("expected error to contains %q, got %q", "key mismatch", err.Error())
		}
	})

	t.Logf("before stop: %d fds", nfds())
	m.Stop()

	// see if we got any error logs
	// from the manager while it was running
	logged := logbuf.Bytes()
	if len(logged) > 0 {
		lines := strings.Split(string(logged), "\n")
		for i := range lines {
			t.Error(lines[i])
		}
	}
	// wait for Serve() to return nil
	<-stopped
	t.Logf("at end: %d fds", nfds())
}

type split4 struct {
	id   tnproto.ID
	key  tnproto.Key
	port int // local port on which the tenant is bound
}

func (s *split4) Split(tbl expr.Node, h plan.TableHandle) (plan.Subtables, error) {
	out := make(plan.SubtableList, 4)
	out[0].Handle = h
	out[0].Table = &expr.Table{
		Binding: expr.Bind(tbl, "copy-0"),
	}
	// get test coverage of using a LocalTransport
	// for one of the UnionMap sub-plans
	out[0].Transport = &plan.LocalTransport{}
	for i := 1; i < 4; i++ {
		out[i].Handle = h
		out[i].Table = &expr.Table{
			// textually the same as the original input,
			// but not == in terms of strict equality
			Binding: expr.Bind(tbl, fmt.Sprintf("copy-%d", i)),
		}
		out[i].Transport = &tnproto.Remote{
			ID:   s.id,
			Key:  s.key,
			Net:  "tcp",
			Addr: net.JoinHostPort("localhost", strconv.Itoa(s.port)),
		}
	}
	return out, nil
}

func mksplit(t *testing.T, query string, env plan.Env, split plan.Splitter) *plan.Tree {
	s, err := partiql.Parse([]byte(query))
	if err != nil {
		t.Fatal(err)
	}
	tree, err := plan.NewSplit(s, env, split)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

// when m.Remote is bound to localhost:0 (for testing),
// determine which port it chose
func getport(t *testing.T, m *Manager) int {
	addr := m.remote.Addr()
	tcpa, ok := addr.(*net.TCPAddr)
	if !ok {
		t.Fatalf("bad local manager addr type %T", addr)
	}
	if !tcpa.IP.IsLoopback() {
		t.Fatalf("bad IP; expected loopback; found %d", tcpa.IP)
	}
	return tcpa.Port
}

func socketPair(t testing.TB) (net.Conn, net.Conn) {
	a, b, err := usock.SocketPair()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("socket pair: (%d, %d)", usock.Fd(a), usock.Fd(b))
	return a, b
}

// begin execution of a split query and yield the
// returned data.
func splitquery(t *testing.T, query string, m *Manager, id tnproto.ID, key tnproto.Key) (io.ReadCloser, io.ReadCloser) {
	tree := mksplit(t, query, stubenv{}, &split4{id: id, key: key, port: getport(t, m)})
	me, there := socketPair(t)

	t.Logf("split plan: %s", tree.String())

	rc, err := m.Do(id, key, tree, tnproto.OutputRaw, there)
	there.Close()
	if err != nil {
		me.Close()
		if rc != nil {
			rc.Close()
		}
		t.Fatal(err)
	}
	return rc, me
}

func openfds(t *testing.T) []string {
	fi, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(fi))
	for i := range fi {
		name, err := os.Readlink(filepath.Join("/proc/self/fd", fi[i].Name()))
		if err != nil {
			t.Log(err)
			continue
		}
		out = append(out, name)
	}
	return out
}

func testEqual(t *testing.T, query string, m *Manager, id tnproto.ID, key tnproto.Key, want []string, count int, scan int64) {
	rc, qr := splitquery(t, query, m, id, key)
	var row, wantrow ion.Datum
	var st ion.Symtab

	out, err := io.ReadAll(qr)
	if err != nil {
		t.Fatal(err)
	}
	qr.Close()
	var stats plan.ExecStats
	err = Check(rc, &stats)
	if err != nil {
		t.Error(err)
		return
	}
	if scan != 0 && stats.BytesScanned != scan {
		t.Errorf("%d bytest scanned; wanted %d", stats.BytesScanned, scan)
	}
	t.Logf("%d hits, %d misses", stats.CacheHits, stats.CacheMisses)

	rownum := 0
	for len(out) > 0 {
		if ion.TypeOf(out) == ion.NullType && ion.SizeOf(out) > 1 {
			// skip nops
			out = out[ion.SizeOf(out):]
			continue
		}
		row, out, err = ion.ReadDatum(&st, out)
		if err != nil {
			t.Fatalf("reading row %d: %s", rownum, err)
		}
		if len(want) == 0 && !row.Empty() {
			if rownum >= count {
				t.Errorf("extra row %d: %v", rownum, row)
			}
			rownum++
			continue
		}
		wantrow, err = ion.FromJSON(&st, json.NewDecoder(strings.NewReader(want[0])))
		if err != nil {
			t.Fatalf("bad test table entry %q %s", want[0], err)
		}
		if !row.Equal(wantrow) {
			t.Errorf("row %d: got : %#v", rownum, row)
			t.Errorf("row %d: want: %#v", rownum, wantrow)
		}
		want = want[1:]
		rownum++
	}
	if len(want) > 0 {
		t.Errorf("failed to match %d trailing rows", len(want))
	}
}

func testCancel(t *testing.T, m *Manager) {
	here, there := socketPair(t)
	defer there.Close()
	id, key := randpair()
	// this plan should loop indefinitely until
	// it is canceled by the
	rc, err := m.Do(id, key, mkplan(t, `SELECT * FROM HANG('../testdata/parking.10n')`), tnproto.OutputRaw, here)
	here.Close()
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	rc.Close()
	// this will hang unless the remote end
	// does the right thing and hangs up after
	// noticing the cancellation
	_, err = io.Copy(io.Discard, there)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cancel took %s", time.Since(start))
}

// Benchmark a trivial query;
// this gives us a sense of what
// the overhead is of Manager.Do()
// independent of actual query execution.
func BenchmarkSendPlan(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		b.Fatal(err)
	}
	var logbuf bytes.Buffer
	m := NewManager([]string{"go", "run", "bench_stub.go", "worker"},
		WithGCInterval(time.Hour),
		WithLogger(log.New(&logbuf, "manager-log: ", 0)),
		WithRemote(l),
	)
	// if bwrap(1) is installed,
	// test that sandboxing works
	m.Sandbox = CanSandbox()
	m.CacheDir = b.TempDir()
	defer m.Stop()
	blocks := []int{
		1, 100, 10000, 100000,
	}
	for _, count := range blocks {
		for _, ranges := range []bool{true, false} {
			name := fmt.Sprintf("%d-blocks", count)
			if ranges {
				name += "+ranges"
			}
			env := &benchenv{blocks: count, ranges: ranges}
			b.Run(name, func(b *testing.B) {
				id, key := randpair()
				s, err := partiql.Parse([]byte("SELECT * FROM input LIMIT 1"))
				if err != nil {
					b.Fatal(err)
				}
				tree, err := plan.New(s, env)
				if err != nil {
					b.Fatal(err)
				}
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					here, there, err := usock.SocketPair()
					if err != nil {
						b.Fatal(err)
					}
					rc, err := m.Do(id, key, tree, tnproto.OutputRaw, there)
					there.Close()
					if err != nil {
						b.Fatal(err)
					}
					_, err = io.ReadAll(here)
					here.Close()
					if err != nil {
						b.Fatal(err)
					}
					var stats plan.ExecStats
					err = Check(rc, &stats)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

type benchenv struct {
	blocks int
	ranges bool
}

type benchHandle struct {
	*blob.List
}

func (b *benchHandle) Open(_ context.Context) (vm.Table, error) {
	panic("benchHandle.Open()")
}

func (b *benchHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	b.List.Encode(dst, st)
	return nil
}

func (b *benchenv) Stat(_ expr.Node, _ *plan.Hints) (plan.TableHandle, error) {
	// produce N fake compressed blobs
	// with data that is reasonably sized
	lst := make([]blob.Interface, b.blocks)
	for i := range lst {
		lst[i] = &blob.Compressed{
			From: &blob.URL{
				Value: "https://s3.amazonaws.com/a-very-long/path-to-the-object/finally.ion.zst",
				Info: blob.Info{
					ETag:         "\"abc123xyzandmoreetagstringhere\"",
					Size:         1234567,
					Align:        1024 * 1024,
					LastModified: date.Now(),
				},
			},
			Trailer: &blockfmt.Trailer{
				Version:    1,
				Offset:     1234500,
				Algo:       "zstd",
				BlockShift: 20,
				// common case for the new format
				// will be ~100 chunks and one block descriptor
				Blocks: []blockfmt.Blockdesc{{
					Offset: 0,
					Chunks: 100,
				}},
			},
		}
	}
	return &benchHandle{&blob.List{lst}}, nil
}
