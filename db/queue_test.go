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

package db

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type memItem struct {
	path, etag string
	complete   func()
}

func (m *memItem) Path() string { return m.path }
func (m *memItem) ETag() string { return m.etag }

type memqueue struct {
	in          chan *memItem
	retry       []*memItem
	outstanding []*memItem
	ticker      *time.Ticker
}

func newQueue() *memqueue {
	return &memqueue{in: make(chan *memItem, 1)}
}

func (m *memqueue) push(path, etag string, complete func()) {
	m.in <- &memItem{path, etag, complete}
}

func (m *memqueue) Finalize(item QueueItem, status QueueStatus) {
	want := item.(*memItem)
	idx := -1
	for i := 0; i < len(m.outstanding); i++ {
		if m.outstanding[i] == want {
			idx = i
			break
		}
	}
	if idx < 0 {
		panic("Finalize of non-existent queue item " + item.Path())
	}
	m.outstanding[idx] = m.outstanding[len(m.outstanding)-1]
	m.outstanding = m.outstanding[:len(m.outstanding)-1]
	if status != StatusOK {
		m.retry = append(m.retry, want)
	} else if want.complete != nil {
		want.complete()
	}
}

func (m *memqueue) close() {
	close(m.in)
}

func (m *memqueue) Next(pause time.Duration) (QueueItem, error) {
	if len(m.retry) > 0 {
		tail := m.retry[len(m.retry)-1]
		m.retry = m.retry[:len(m.retry)-1]
		m.outstanding = append(m.outstanding, tail)
		return tail, nil
	}
	if pause < 0 {
		item, ok := <-m.in
		if !ok {
			return nil, io.EOF
		}
		m.outstanding = append(m.outstanding, item)
		return item, nil
	}
	if m.ticker == nil {
		m.ticker = time.NewTicker(pause)
	} else {
		m.ticker.Reset(pause)
	}
	select {
	case item, ok := <-m.in:
		m.ticker.Stop()
		if !ok {
			return nil, io.EOF
		}
		m.outstanding = append(m.outstanding, item)
		return item, nil
	case <-m.ticker.C:
		m.ticker.Stop()
		return nil, nil
	}
}

// run queue in the background and clean it up
// on test completion, ensuring errors are checked
func runQueue(t *testing.T, r *QueueRunner, q *memqueue) {
	final := make(chan error, 1)
	go func() {
		final <- r.Run(q)
	}()
	t.Cleanup(func() {
		q.close()
		err := <-final
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestQueue(t *testing.T) {
	for _, conf := range []struct {
		batch int
		scan  bool
	}{
		{
			batch: 1,
			scan:  true,
		},
		{
			batch: 10,
			scan:  true,
		},
		{
			batch: 3,
			scan:  true,
		},
		{
			batch: 1,
			scan:  false,
		},
		{
			batch: 3,
			scan:  false,
		},
		{
			batch: 10,
			scan:  false,
		},
	} {
		t.Run(fmt.Sprintf("batch=%d,scan=%v", conf.batch, conf.scan), func(t *testing.T) {
			testQueue(t, conf.batch, conf.scan)
		})
	}
}

func testQueue(t *testing.T, batchsize int, scan bool) {
	q := newQueue()
	r := &QueueRunner{
		Logf:          t.Logf,
		BatchSize:     batchsize,
		BatchInterval: time.Millisecond,
	}

	check := func(err error) {
		if err != nil {
			t.Helper()
			t.Fatal(err)
		}
	}

	checkFiles(t)
	tmpdir := t.TempDir()
	for _, dir := range []string{
		filepath.Join(tmpdir, "aabb"),
		filepath.Join(tmpdir, "aacc"),
	} {
		check(os.MkdirAll(dir, 0750))
	}

	dfs := NewDirFS(tmpdir)
	defer dfs.Close()
	dfs.Log = t.Logf

	var queued sync.WaitGroup
	push := func(name, etag string) {
		queued.Add(1)
		q.push(name, etag, queued.Done)
	}

	create := func(name, text string) {
		etag, err := dfs.WriteFile(name, []byte(text))
		if err != nil {
			t.Helper()
			t.Fatal(err)
		}
		push(dfs.Prefix()+name, etag)
	}

	check(WriteDefinition(dfs, "db0", &Definition{
		Name: "narrow",
		Inputs: []Input{
			{Pattern: "file://aabb/file*.json"},
		},
	}))
	// should get a superset of narrow
	check(WriteDefinition(dfs, "db1", &Definition{
		Name: "wide",
		Inputs: []Input{
			{Pattern: "file://aa*/file*.json"},
		},
	}))

	owner := newTenant(dfs)
	r.Owner = owner
	r.Conf = Builder{
		Align:        1024,
		NewIndexScan: scan,
		GCLikelihood: 2,
	}

	runQueue(t, r, q)

	create("aabb/file0.json", `{"name": "aabb/file0.json", "value": 0}`)
	create("aacc/file0.json", `{"name": "aacc/file0.json", "value": 1}`)
	// bad file; shouldn't permanently stop ingest:
	create("aabb/file1.json", `{"name": "aabb/file1.json"`)
	// push a file that doesn't exist; this should be ignored
	push("aabb/file0.json", "abcdefg")

	queued.Wait()

	checkIndex := func(db, table string, want map[string]bool) {
		idx, err := OpenIndex(dfs, "db0", "narrow", owner.Key())
		if err != nil {
			t.Fatal(err)
		}
		idx.Inputs.Backing = dfs
		check(idx.Inputs.Walk("", func(name, etag string, id int) bool {
			accept, ok := want[name]
			if !ok {
				t.Errorf("unexpected file %q", name)
				return true
			}
			if accept != (id >= 0) {
				t.Errorf("file %q: accepted: %v", name, id >= 0)
			}
			return true
		}))
	}
	checkIndex("db0", "narrow", map[string]bool{
		"file://aabb/file0.json": true,
		"file://aabb/file1.json": false,
	})
	checkIndex("db1", "wide", map[string]bool{
		"file://aabb/file0.json": true,
		"file://aabb/file1.json": false,
		"file://aacc/file0.json": true,
	})
}
