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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SnellerInc/sneller/aws/s3"
	"github.com/SnellerInc/sneller/ion/blockfmt"

	"golang.org/x/exp/slices"
)

const (
	// DefaultBatchSize is the default queue
	// batching size that is used when none is set.
	DefaultBatchSize = 100 * mega
)

// QueueStatus indicates the processing
// status of a QueueItem.
type QueueStatus int32

const (
	// StatusOK indicates that a QueueItem
	// was processed completely.
	StatusOK = iota
	// StatusTryAgain indicates that a QueueItem
	// was not processed, and it should be re-tried
	// shortly.
	StatusTryAgain
	// StatusWriteError indicates taht a QueueItem
	// was not processed, and it should be re-tried
	// after a delay.
	StatusWriteError
)

// QueueItem represents an item
// in a notification queue.
type QueueItem interface {
	// Path should return the full path
	// of the file, including the fs prefix.
	Path() string
	// ETag should return the ETag of
	// the file.
	ETag() string
	// Size should return the size of
	// the file at Path in bytes.
	Size() int64
	// EventTime should return the time
	// at which the queue item was inserted
	// into the queue. EventTime is used to
	// compute statistics about total queue delays.
	EventTime() time.Time
}

type Queue interface {
	// Close should be called when the
	// runner has finished processing items
	// from the queue (usually due to receiving
	// and external signal to stop processing events).
	// Close should only be called after all events
	// returned by Next have been processed via calls
	// to Finalize.
	io.Closer
	// Next should return the next item
	// in the queue. If the provided pause
	// duration is non-negative, then Next
	// should block for up to the provided duration
	// to produce a new value. If pause is negative,
	// then Next should block until it can return
	// a non-nil QueueItem value or an EOF error.
	// Next should return (nil, io.EOF) is the queue
	// has been closed and no further processing should
	// be performed.
	//
	// As an optimization, the returned QueueItem
	// can implement fs.File, which will obviate
	// the need for the caller to perform additional
	// I/O to produce a file handle associated with
	// the QueueItem.
	Next(pause time.Duration) (QueueItem, error)
	// Finalize is called to return the final status
	// of a QueueItem that was previously returned by
	// ReadInputs. If status is anything other than
	// StatusOK, then the Queue should arrange for the
	// item to be read by a future call to ReadInputs.
	//
	// Finalize may panic if Queue.Close has been called.
	Finalize(item QueueItem, status QueueStatus)
}

// QueueRunner encapsulates the state
// required to process a single queue.
type QueueRunner struct {
	Owner Tenant
	// Conf is the configuration
	// used for building tables.
	Conf Builder

	// Logf is used to log errors encountered
	// while processing entries from a queue.
	// Logf may be nil.
	Logf func(f string, args ...interface{})

	// Open is a hook that can be used to override
	// how queue items are opened.
	// The default behavior is to use ifs.Open(item.Path())
	Open func(ifs InputFS, item QueueItem) (fs.File, error)

	// TableRefresh is the interval at which
	// table definitions are refreshed.
	// If TableRefresh is less than or equal
	// to zero, then tables are refreshed every minute.
	TableRefresh time.Duration

	// BatchSize is the maximum number of bytes
	// that the QueueRunner will attempt to read
	// from a Queue in Run before comitting the
	// returned items. Batches may be smaller than
	// BatchSize due to the expiration of BatchInterval
	// or due to receiving an error from the queue
	// after batching a non-zero number of items.
	// (The size of a batch is computed by summing
	// the QueueItem.Size values from each QueueItem
	// returned from Next.)
	//
	// If BatchSize is less than or equal to zero,
	// then DefaultBatchSize is used instead.
	//
	// See also: BatchInterval
	BatchSize int64

	// BatchInterval is the maximum amount of
	// time the queue should wait for successive
	// calls to Queue.Next to accumulate BatchSize items.
	//
	// See also: BatchSize
	BatchInterval time.Duration

	// IOErrDelay determines how long queue processing
	// will pause if it encounters an I/O error from
	// the backing filesystem.
	IOErrDelay time.Duration

	// scratch space for processing batches
	inputs []QueueItem
	status []QueueStatus
}

func (q *QueueRunner) max() int64 {
	if q.BatchSize > 0 {
		return q.BatchSize
	}
	return DefaultBatchSize
}

// Merge returns the more sever status
// of either s or other.
func (s QueueStatus) Merge(other QueueStatus) QueueStatus {
	if s > other {
		return s
	}
	return other
}

func (s *QueueStatus) atomicMerge(other QueueStatus) {
	for {
		got := atomic.LoadInt32((*int32)(s))
		want := QueueStatus(got).Merge(other)
		if got == int32(want) || atomic.CompareAndSwapInt32((*int32)(s), got, int32(want)) {
			break
		}
	}
}

func errResult(err error) QueueStatus {
	if err == nil {
		return StatusOK
	} else if err == ErrBuildAgain {
		return StatusTryAgain
	}
	return StatusWriteError
}

func (q *QueueRunner) delay() {
	if q.IOErrDelay > 0 {
		time.Sleep(q.IOErrDelay)
	}
}

// perform the equivalent of infs.Open(name),
// but take care to skip the I/O of the FS implementation
// can just produce a handle directly
func (q *QueueRunner) open(infs InputFS, name string, item QueueItem) (fs.File, error) {
	// an s3-specific optimization: don't do any
	// I/O if we have enough information to produce
	// an s3.File handle already
	if b, ok := infs.(*S3FS); ok {
		f := s3.NewFile(b.Key, b.Bucket, name, item.ETag(), item.Size())
		f.Client = b.Client
		return f, nil
	}
	return infs.Open(name)
}

// populate dst from q.inputs based on
// the patterns in def and the configs in bld
// (bld[i] corresponds to def.Tables[i])
//
// this is supposed to be safe to call from multiple goroutines
func (q *QueueRunner) filter(bld []Builder, def *Definition, dst *[]batch) {
	*dst = (*dst)[:0]
	ind := make(map[string]int) // index into dst
	get := func(tbl []byte) *batch {
		i, ok := ind[string(tbl)]
		if ok {
			return &(*dst)[i]
		}
		tblname := def.Name
		if tblname != string(tbl) {
			tblname = string(tbl)
		}
		ind[tblname] = len(*dst)
		*dst = append(*dst, batch{table: tblname})
		return &(*dst)[len(*dst)-1]
	}
	var mr matcher
outer:
	for i := range q.inputs {
		p := q.inputs[i].Path()
		etag := q.inputs[i].ETag()
		for j := range def.Tables {
			var dst *batch
			def := def.Tables[j]
			for k := range def.Inputs {
				err := mr.match(def.Inputs[k].Pattern, p, def.Name)
				if err != nil || !mr.found {
					continue
				}
				if dst != nil {
					// we already matched the input, but
					// make sure we are not producing
					// multiple tables from one input
					if dst.table != string(mr.result) {
						q.Logf("conflicting table name: %q or %q?", dst.table, mr.result)
						dst.err = fmt.Errorf("conflicting table name")
						continue outer
					}
					continue
				}
				dst = get(mr.result)
				if dst.def == nil {
					dst.def = def
					dst.conf = &bld[j]
				} else if dst.def != def {
					q.Logf("duplicate table definition for %q", dst.table)
					dst.err = fmt.Errorf("duplicate table definition")
					continue outer
				}
				infs, name, err := q.Owner.Split(p)
				if err != nil {
					dst.err = err
					continue outer
				}
				f, err := q.open(infs, name, q.inputs[i])
				if err != nil {
					if errors.Is(err, fs.ErrNotExist) {
						q.Logf("ignoring %q (doesn't exist)", name)
						continue outer
					}
					dst.err = err
					continue outer
				}
				info, err := f.Stat()
				if err != nil {
					f.Close()
					dst.err = err
					continue outer
				}
				gotEtag, err := infs.ETag(name, info)
				if err != nil {
					f.Close()
					dst.err = err
					continue outer
				}
				if etag != gotEtag {
					f.Close()
					q.Logf("ignoring %q due to etag mismatch (want %q got %q)", name, etag, gotEtag)
					continue outer
				}
				fm, err := bld[j].Format(def.Inputs[k].Format, p, def.Inputs[k].Hints)
				if err != nil {
					dst.err = err
					continue outer
				}
				dst.note(q.inputs[i].EventTime())
				dst.indirect = append(dst.indirect, i)
				dst.filtered = append(dst.filtered, blockfmt.Input{
					Path: p,
					ETag: etag,
					Size: info.Size(),
					R:    f,
					F:    fm,
				})
			}
		}
	}
}

func (b *batch) note(qtime time.Time) {
	if qtime.IsZero() {
		return
	}
	if b.earliest.IsZero() || qtime.Before(b.earliest) {
		b.earliest = qtime
	}
	if b.latest.IsZero() || qtime.After(b.latest) {
		b.latest = qtime
	}
}

type batch struct {
	table            string
	def              *TableDefinition
	conf             *Builder
	filtered         []blockfmt.Input
	indirect         []int     // indices into Queue.items[] for each of filtered
	earliest, latest time.Time // modtimes for batch
	err              error     // error found during filter
}

// IndexCache is an opaque cache for index objects.
type IndexCache struct {
	m     map[string]int
	items []*blockfmt.Index
}

// init allows an index for a table to be stored
// in the cache. invalidate or overwrite will do
// nothing if init has not been called for that
// table. This method is not thread-safe.
func (c *IndexCache) init(table string) {
	if c != nil {
		_, ok := c.m[table]
		if ok {
			// already inited
			return
		}
		if c.m == nil {
			c.m = make(map[string]int)
		}
		c.m[table] = len(c.items)
		c.items = append(c.items, nil)
	}
}

func (c *IndexCache) get(table string) *blockfmt.Index {
	if c == nil {
		return nil
	}
	i, ok := c.m[table]
	if !ok {
		return nil
	}
	return c.items[i]
}

func (c *IndexCache) invalidate(table string) {
	c.overwrite(table, nil)
}

func (c *IndexCache) overwrite(table string, value *blockfmt.Index) {
	if c != nil {
		i, ok := c.m[table]
		if ok {
			c.items[i] = value
		}
	}
}

func (q *QueueRunner) runDatabase(def *Definition, cache *IndexCache) {
	conf := make([]Builder, len(def.Tables))
	for i := range def.Tables {
		// clone the config and add features;
		// note that runDatabase is invoked in separate
		// goroutines for each table, so we need to
		// deep-copy these structures to keep things race-free
		conf[i] = q.Conf
		conf[i].SetFeatures(def.Tables[i].Features)
	}
	var dst []batch
	q.filter(conf, def, &dst)
	for i := range dst {
		cache.init(dst[i].table)
	}
	var wg sync.WaitGroup
	wg.Add(len(dst))
	for i := range dst {
		go func(dst *batch) {
			defer wg.Done()
			q.runTable(def.Name, dst, cache)
		}(&dst[i])
	}
	wg.Wait()
}

func (q *QueueRunner) runTable(db string, dst *batch, cache *IndexCache) {
	sizeof := func(lst []blockfmt.Input) int64 {
		out := int64(0)
		for i := range lst {
			out += lst[i].Size
		}
		return out
	}

	table := dst.table
	conf := dst.conf
	err := dst.err
	if err == nil && len(dst.filtered) > 0 {
		err = conf.Append(q.Owner, db, table, dst.def, dst.filtered, cache)
		if err == nil {
			q.logf("table %s/%s inserted %d objects %d source bytes mindelay %s maxdelay %s",
				db, table, len(dst.filtered), sizeof(dst.filtered), time.Since(dst.latest), time.Since(dst.earliest))
		}
	}
	if err != nil {
		// for safety, invalidate the cache whenever
		// we encounter an error; the index file is
		// likely not in a consistent state
		if err != ErrBuildAgain {
			cache.invalidate(table)
		}
		q.logf("updating %s/%s: %s", db, table, err)
	}

	// atomically merge status codes back into parent
	status := errResult(err)
	for _, j := range dst.indirect {
		q.status[j].atomicMerge(status)
	}
}

func (q *QueueRunner) logf(f string, args ...interface{}) {
	if q.Logf != nil {
		q.Logf(f, args...)
	}
}

func (q *QueueRunner) tableRefresh() time.Duration {
	if q.TableRefresh > 0 {
		return q.TableRefresh
	}
	return time.Minute
}

func bounce(q Queue, lst []QueueItem, st QueueStatus) {
	for i := range lst {
		q.Finalize(lst[i], st)
	}
}

// set q.inputs to a list of items
// gathered from the queue using the
// provided batching parameters
func (q *QueueRunner) gather(in Queue) error {
	q.inputs = q.inputs[:0]

	// first item: block forever
	first, err := in.Next(-1)
	if err != nil {
		return err
	}
	if first == nil {
		return fmt.Errorf("Queue implementation bug: Next(-1) should block")
	}
	const hardlimit = 5000
	total := first.Size()
	// keep gathering items up to the max batch size
	// or the max delay time
	q.inputs = append(q.inputs, first)
	end := time.Now().Add(q.BatchInterval)
	for total < q.max() && len(q.inputs) < hardlimit {
		u := time.Until(end)
		if u <= 0 {
			break
		}
		item, err := in.Next(u)
		if err != nil || item == nil {
			break
		}
		q.inputs = append(q.inputs, item)
		total += item.Size()
	}
	return nil
}

type dbinfo struct {
	def   *Definition
	cache IndexCache
}

// Run processes entries from in until ReadInputs returns io.EOF,
// at which point it will call in.Close.
func (q *QueueRunner) Run(in Queue) error {
	var lastRefresh time.Time
	var subdefs []dbinfo
readloop:
	for {
		err := q.gather(in)
		if err != nil {
			cerr := in.Close()
			if err == io.EOF {
				err = cerr
			}
			return err
		}
		if time.Since(lastRefresh) > q.tableRefresh() {
			err := q.updateDefs(&subdefs)
			if err != nil {
				q.logf("updating table definitions: %s", err)
				bounce(in, q.inputs, StatusWriteError)
				q.delay()
				continue readloop
			}
			lastRefresh = time.Now()
		}
		q.runBatches(in, subdefs)
	}
}

func (q *QueueRunner) runBatches(in Queue, defs []dbinfo) {
	var wg sync.WaitGroup
	q.status = slices.Grow(q.status[:0], len(q.inputs))[:len(q.inputs)]
	for i := range q.status {
		q.status[i] = StatusOK
	}
	for i := range defs {
		wg.Add(1)
		go func(def *Definition, cache *IndexCache) {
			defer wg.Done()
			q.runDatabase(def, cache)
		}(defs[i].def, &defs[i].cache)
	}
	// wait for q.status[*] to be updated (atomically!)
	// by runDatabase()
	wg.Wait()
	for i := range q.status {
		in.Finalize(q.inputs[i], q.status[i])
	}
}

func (q *QueueRunner) updateDefs(defs *[]dbinfo) error {
	dir, err := q.Owner.Root()
	if err != nil {
		return err
	}
	*defs = (*defs)[:0]
	dbs, err := fs.ReadDir(dir, "db")
	if err != nil {
		return err
	}
	for i := range dbs {
		if !dbs[i].IsDir() {
			continue
		}
		db := dbs[i].Name()
		def, err := OpenDefinition(dir, db)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		*defs = append(*defs, dbinfo{def: def})
	}
	return nil
}
