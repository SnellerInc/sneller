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

package db

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"runtime/trace"
	"strings"
	"sync"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// DefaultMinMerge is the default minimum merge size.
//
// Rationale: it looks like S3 server-side copy
// will run at about 250MB/s, so 50MB done as a
// synchronous server-side copy will introduce
// 200ms of ingest latency over and above the baseline.
// That seems like a reasonable maximum overhead.
// (Keep in mind this is 50MB compressed, so potentially
// a few hundred MB decompressed.)
const DefaultMinMerge = 50 * 1024 * 1024

// DefaultTargetMergeSize is the default target size for compacted packfiles.
const DefaultTargetMergeSize = 1 * giga

// DefaultRangeMultiple is the default
// multiple of the chunk alignment at which
// we write out metadata.
const DefaultRangeMultiple = 100

const (
	kilo = 1024
	mega = kilo * kilo
	giga = kilo * mega
)

// DefaultMaxInlineBytes is the default
// number of decompressed bytes that we
// reference in blockfmt.Index.Inline before
// flushing references to blockfmt.Index.Indirect.
const DefaultMaxInlineBytes = 100 * giga

// DefaultAlgo is the default compression algorithm
// for compressing data blocks.
const DefaultAlgo = "zion+iguana_v0/specialized"

// ErrBuildAgain is returned by db.Config.Sync
// when only some of the input objects were successfully
// ingested.
var ErrBuildAgain = errors.New("partial db update")

// Config is a set of configuration items
// for synchronizing an Index to match
// a specification from a Definition.
type Config struct {
	// Algo is the compression algorithm
	// used for producing output data blocks.
	// (See [blockfmt.CompressorByName].)
	// If Algo is the empty string, Config
	// uses DefaultAlgo instead.
	Algo string
	// Align is the alignment of new
	// blocks to be produced in objects
	// inserted into the index.
	Align int
	// RangeMultiple is the multiple of Align
	// at which we write out metadata.
	// If RangeMultiple is zero, it defaults to
	// DefaultRangeMultiple
	RangeMultiple int
	// MinMergeSize is the base merge
	// size of objects. If MinMergeSize is zero,
	// then DefaultMinMerge is used.
	MinMergeSize int
	// TargetMergeSize is the target size of
	// files that are compacted into larger packfiles.
	// If TargetMergeSize is zero, then DefaultTargetMergeSize is used.
	TargetMergeSize int
	// MinInputBytesPerCPU, if non-zero, determines the minimum
	// number of input bytes necessary to cause the conversion
	// process to decide to use an additional CPU core.
	// For example, if MinInputBytesPerCPU is 512kB, then 3MB of input
	// data would use 6 CPU cores (provided GOMAXPROCS is at least this high).
	// See blockfmt.MinInputBytesPerCPU
	MinInputBytesPerCPU int64
	// Force forces a full index rebuild
	// even when the input appears to be up-to-date.
	Force bool
	// Fallback determines the format for
	// objects when the object format is not
	// obvious from the file extension.
	Fallback func(name string) blockfmt.RowFormat
	// MaxScanObjects is the maximum number
	// of objects to be committed in a single Scan operation.
	// If MaxScanObjects is less than or equal to zero,
	// it is ignored and no limit is applied.
	MaxScanObjects int
	// MaxScanBytes is the maximum number
	// of bytes to ingest in a single Scan or Sync operation
	// (not including merging).
	// If MaxScanBytes is less than or equal to zero,
	// it is ignored and no limit is applied.
	MaxScanBytes int64
	// MaxScanTime is the maximum amount of time
	// to spend listing objects before deciding
	// to bail out of a scan.
	MaxScanTime time.Duration

	// NewIndexScan, if true, enables scanning
	// for newly-created index objects.
	NewIndexScan bool

	// MaxInlineBytes is the maximum number
	// of (decompressed) data bytes for which
	// we should store references directly in
	// blockfmt.Index.Inline.
	// If this value is zero, then DefaultMaxInlineBytes
	// is used instead.
	MaxInlineBytes int64
	// TargetRefSize is the target size for stored
	// indirect references. If this value is zero,
	// a reasonable default is used.
	TargetRefSize int64

	// GCMaxDelay is the longest amount of time that
	// a gc cycle will spend blocking a batch insert operation.
	GCMaxDelay time.Duration
	// GCMinimumAge is the minimum time that
	// a packed file should be left around after
	// it has been dereferenced.
	// See blockfmt.Index.ToDelete.Expiry for
	// how this value is used.
	GCMinimumAge time.Duration

	// InputMinimumAge is the mininum time
	// that an input file leaf should be left
	// around after it is no longer referenced.
	// See blockfmt.Index.ToDelete.Expiry
	InputMinimumAge time.Duration

	// Logf, if non-nil, will be where
	// the builder will log build actions
	// as it is executing. Logf must be
	// safe to call from multiple goroutines
	// simultaneously.
	Logf func(f string, args ...interface{})

	Verbose bool
}

func (c *Config) minMergeSize() int64 {
	if c.MinMergeSize > 0 {
		return int64(c.MinMergeSize)
	}
	return DefaultMinMerge
}

func (c *Config) targetMerge() int {
	if c.TargetMergeSize <= 0 {
		return DefaultTargetMergeSize
	}
	return c.TargetMergeSize
}

// Format picks the row format for an object
// based on an explicit format hint and the object name.
// The following are tried, in order:
//  1. If 'chosen' is the name of a known format,
//     then that format is returned.
//  2. If 'name' has a suffix that indicates a known format,
//     then that format is returned.
//  3. If c.Fallback is non-nil, then Fallback(name) is returned.
//
// Otherwise, Format returns nil.
func (c *Config) Format(chosen, name string, hints []byte) (blockfmt.RowFormat, error) {
	if chosen != "" {
		if f := blockfmt.SuffixToFormat["."+chosen]; f != nil {
			return f(hints)
		}
	}
	for suff, f := range blockfmt.SuffixToFormat {
		if strings.HasSuffix(name, suff) {
			return f(hints)
		}
	}
	if c.Fallback != nil {
		return c.Fallback(name), nil
	}
	return nil, nil
}

func (c *Config) logf(f string, args ...interface{}) {
	if c.Logf != nil {
		c.Logf(f, args...)
	}
}

type tableState struct {
	cache struct {
		value *blockfmt.Index
		etag  string
	}
	def       *Definition
	conf      Config
	owner     Tenant
	ofs       OutputFS
	db, table string
	shouldGC  bool
}

func (st *tableState) logf(f string, args ...any) {
	st.conf.logf("%s/%s: %s", st.db, st.table, fmt.Sprintf(f, args...))
}

func (st *tableState) invalidate() {
	st.cache.value = nil
	st.cache.etag = ""
}

func (st *tableState) overwrite(idx *blockfmt.Index, etag string) {
	st.cache.value = idx
	st.cache.etag = etag
}

func (st *tableState) runGC(ctx context.Context, idx *blockfmt.Index) {
	st.preciseGC(idx)
	if st.shouldGC {
		err := st.fullGC(ctx, idx)
		if err != nil {
			st.logf("full gc: %s", err)
		}
		st.shouldGC = err == nil
		return
	}
}

func (c *Config) open(db, table string, owner Tenant) (*tableState, error) {
	ifs, err := owner.Root()
	if err != nil {
		return nil, err
	}
	ofs, ok := ifs.(OutputFS)
	if !ok {
		return nil, fmt.Errorf("root %T is read-only", ifs)
	}
	def, err := OpenDefinition(ifs, db, table)
	if errors.Is(err, fs.ErrNotExist) {
		def = &Definition{}
	} else if err != nil {
		return nil, err
	}
	ts := &tableState{
		def:   def,
		conf:  *c, // copy config so we can update it w/ features
		owner: owner,
		ofs:   ofs,
		db:    db,
		table: table,
	}
	ts.conf.SetFeatures(def.Features)
	return ts, nil
}

func (st *tableState) index(ctx context.Context) (*blockfmt.Index, error) {
	if st.cache.value != nil {
		return st.cache.value, nil
	}
	defer trace.StartRegion(ctx, "load-index").End()
	ipath := IndexPath(st.db, st.table)
	idx, info, err := openIndex(st.ofs, ipath, st.owner.Key(), 0)
	if err != nil {
		return nil, err
	}
	etag, err := st.ofs.ETag(ipath, info)
	if err != nil {
		return nil, err
	}
	st.overwrite(idx, etag)
	// flag this table as requiring gc on the first load,
	// since re-loading a table is an indication that we
	// may have encountered and error (and created garbage)
	st.shouldGC = true
	return idx, nil
}

// partitionFor returns the partition prefix for
// a packfile object path p, or ("", false) if
// the partition could not be determined from
// the object path.
func (st *tableState) partitionFor(p string) (string, bool) {
	p, _ = path.Split(p)
	if !strings.HasSuffix(p, "/") {
		return "", false
	}
	p = p[:len(p)-1]
	trim := []string{"db/", st.db, "/", st.table}
	for _, pre := range trim {
		if !strings.HasPrefix(p, pre) {
			return "", false
		}
		p = p[len(pre):]
	}
	p = strings.TrimPrefix(p, "/")
	return p, true
}

func inlineToID(idx *blockfmt.Index, inline int) int {
	if inline >= len(idx.Inline) {
		return -1
	}
	return idx.Indirect.OrigObjects() + inline
}

// findPrepend finds the first mergeable inline
// object in the inline list belonging to the
// given partition name, returning its index or
// -1 if not found.
func (st *tableState) findPrepend(idx *blockfmt.Index, part string) int {
	for i := len(idx.Inline) - 1; i >= 0; i-- {
		p, ok := st.partitionFor(idx.Inline[i].Path)
		if !ok || p != part {
			continue
		}
		if idx.Inline[i].Size >= st.conf.minMergeSize() {
			// anything prior is also too big
			break
		}
		return i
	}
	return -1
}

// deleteInline marks the ith inline object for
// deletion. This panics if i is out of range.
func (st *tableState) deleteInline(idx *blockfmt.Index, i int) {
	idx.ToDelete = append(idx.ToDelete, blockfmt.Quarantined{
		Path:   idx.Inline[i].Path,
		Expiry: date.Now().Add(st.conf.GCMinimumAge),
	})
}

func (st *tableState) dedup(ctx context.Context, idx *blockfmt.Index, parts []partition) ([]partition, error) {
	defer trace.StartRegion(ctx, "dedup-inputs").End()
	out := parts[:0]
	nextID := idx.Objects()
	for i := range parts {
		var descID int
		prepend := st.findPrepend(idx, parts[i].name)
		if prepend >= 0 {
			descID = inlineToID(idx, prepend)
		} else {
			descID = nextID
		}
		lst := parts[i].lst
		// try to ensure the Append operations don't block;
		// fetch all the tree leaves in parallel
		idx.Inputs.Prefetch(lst)

		kept := parts[i].lst[:0]
		for i := range lst {
			ret, err := idx.Inputs.Append(lst[i].Path, lst[i].ETag, descID)
			if err != nil {
				if errors.Is(err, blockfmt.ErrETagChanged) {
					// the file at this path has been overwritten
					// with new content; we can't "replace" the old
					// data so there's not much we can do here...
					lst[i].R.Close()
					continue
				}
				return nil, err
			}
			if ret {
				kept = append(kept, lst[i])
			} else {
				lst[i].R.Close()
			}
		}
		if len(kept) == 0 {
			// nothing new to do; keep going
			continue
		}
		if prepend < 0 {
			nextID++
		}
		parts[i].lst = kept
		if prepend >= 0 {
			st.deleteInline(idx, prepend)
		}
		parts[i].prepend = prepend
		out = append(out, parts[i])
	}
	return out, nil
}

// shouldRebuild indicates whether an error
// returned by OpenIndex is an OK condition
// on which to rebuild the index entirely
//
// we're being deliberately conservative here
// because we don't want to do a rebuild on
// a temporary error or something that seriously
// deserves debugging attention
//
// conversely, a non-existing index or an
// index with an out-of-date version are both
// reasonable to rebuild w/o any worry
func shouldRebuild(err error) bool {
	return errors.Is(err, fs.ErrNotExist) ||
		errors.Is(err, blockfmt.ErrIndexObsolete)
}

func (c *Config) shouldScan(def *Definition) bool {
	return c.NewIndexScan && !def.SkipBackfill
}

// append works similarly to Sync,
// but it only works on one database and table at a time,
// and it assumes the list of new elements to be inserted
// has already been computed.
//
// If the index to be updated is currently scanning (see Config.Scan),
// then append will perform some scanning inserts and return ErrBuildAgain.
// append will continue to return ErrBuildAgain until scanning is complete,
// at which point append operations will be accepted. (The caller must continuously
// call append for scanning to occur.)
func (ti *tableInfo) append(ctx context.Context, parts []partition) error {
	idx, err := ti.state.index(ctx)
	if err == nil {
		ti.state.runGC(ctx, idx)
		idx.Inputs.Backing = ti.state.ofs
		if ti.state.shouldScan() && idx.Scanning {
			_, err = ti.state.scan(idx, true)
			if err != nil {
				return err
			}
			// currently rebuilding; please try again
			ti.state.invalidate()
			return ErrBuildAgain
		}
		// trim pre-existing elements from lst
		parts, err = ti.state.dedup(ctx, idx, parts)
		if err != nil {
			return err
		}
		if len(parts) == 0 {
			return nil
		}
		return ti.state.append(ctx, idx, parts)
	}
	if ti.state.shouldScan() && (errors.Is(err, fs.ErrNotExist) || errors.Is(err, blockfmt.ErrIndexObsolete)) {
		idx := &blockfmt.Index{
			Name: ti.state.table,
			Algo: "zstd",
		}
		_, err = ti.state.scan(idx, true)
		if err != nil {
			return err
		}
		return ErrBuildAgain
	}
	if !errors.Is(err, fs.ErrNotExist) {
		// don't try to overwrite an index file
		// if we're not sure what caused the error here;
		// if this is blockfmt.ErrIndexObsolete, then the
		// caller should probably Sync instead
		return err
	}
	return ti.state.append(ctx, nil, parts)
}

func (st *tableState) shouldScan() bool { return st.conf.shouldScan(st.def) }

func (st *tableState) append(ctx context.Context, idx *blockfmt.Index, parts []partition) error {
	var err error
	if len(parts) == 0 {
		if idx == nil {
			err = st.emptyIndex()
		}
	} else {
		err = st.force(ctx, idx, parts)
	}
	if err != nil {
		st.invalidate()
		return fmt.Errorf("force: %w", err)
	}
	return nil
}

// Sync reads each Definition in dst,
// converts the list of input objects
// into the right set of output objects,
// and writes the associated index signed with 'key'.
func (c *Config) Sync(who Tenant, db, tblpat string) error {
	if tblpat == "" {
		tblpat = "*"
	}
	dst, err := who.Root()
	if err != nil {
		return err
	}
	possible, err := fs.Glob(dst, DefinitionPath(db, tblpat))
	if err != nil {
		return err
	}
	var tables []string
	for i := range possible {
		tab, _ := path.Split(possible[i])
		tables = append(tables, path.Base(tab))
	}
	syncTable := func(table string) error {
		st, err := c.open(db, table, who)
		if err != nil {
			return err
		}
		if c.Verbose {
			c.Logf("opened db %q with table %q, tenantID %q", db, table, who.ID())
		}

		fresh := false
		gc := false
		idx, err := st.index(context.Background())
		if err != nil {
			// if the index isn't present
			// or is out-of-date, create a new one
			if shouldRebuild(err) {
				fresh = true
				idx = &blockfmt.Index{
					Name: table,
					Algo: "zstd",
				}
			} else {
				return err
			}
		} else {
			gc = st.preciseGC(idx)
		}
		restart := false
		if !idx.Scanning {
			idx.Cursors = nil
			restart = true
		}
		// we flush the new index on termination
		// if it is a) a new index file,
		// b) it was already in the scanning state,
		// or c) we ran GC and it modified the index
		_, err = st.scan(idx, fresh || !restart || gc)
		if err != nil {
			return err
		}
		if idx.Scanning {
			return ErrBuildAgain
		}
		return nil
	}
	errlist := make([]error, len(tables))
	var wg sync.WaitGroup
	wg.Add(len(tables))
	for i := range tables {
		tab := tables[i]
		go func(i int) {
			defer wg.Done()
			errlist[i] = syncTable(tab)
		}(i)
	}
	wg.Wait()
	return combine(errlist)
}

func combine(lst []error) error {
	var nonnull []error
	for i := range lst {
		if lst[i] != nil {
			nonnull = append(nonnull, lst[i])
		}
	}
	switch len(nonnull) {
	case 0:
		return nil
	case 1:
		return nonnull[0]
	default:
		return fmt.Errorf("%w (and %d more errors)", nonnull[0], len(nonnull)-1)
	}
}

func (c *Config) align() int {
	if c.Align > 0 {
		return c.Align
	}
	return 1024 * 1024
}

func (c *Config) comp() string {
	if c.Algo != "" {
		return c.Algo
	}
	return DefaultAlgo
}

func uuid() string {
	var buf [16]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		// crypto random source is busted?
		panic(err)
	}
	// remove the trailing padding; it is deterministic
	return strings.TrimSuffix(base32.StdEncoding.EncodeToString(buf[:]), "======")
}

func (st *tableState) emptyIndex() error {
	idx := blockfmt.Index{
		Created:  date.Now().Truncate(time.Microsecond),
		Name:     st.table,
		Scanning: st.shouldScan(),
		// no Inline, etc.
	}
	buf, err := blockfmt.Sign(st.owner.Key(), &idx)
	if err != nil {
		return err
	}
	p := IndexPath(st.db, st.table)
	etag, err := st.ofs.WriteFile(p, buf)
	if err == nil {
		st.overwrite(&idx, etag)
	} else {
		st.invalidate()
	}
	return err
}

func (c *Config) flushMeta() int {
	align := c.align()
	if c.RangeMultiple <= 0 {
		return align * DefaultRangeMultiple
	}
	return align * c.RangeMultiple
}

// after failing to read an object,
// update the index state to reflect the fatal errors we encountered
func (st *tableState) updateFailed(ctx context.Context, empty bool, parts []partition) {
	defer trace.StartRegion(ctx, "update-failed").End()
	// invalidate cache so that a reload pulls the previous one
	st.invalidate()

	any := false
	for i := range parts {
		for j := range parts[i].lst {
			if parts[i].lst[j].Err != nil && blockfmt.IsFatal(parts[i].lst[j].Err) {
				any = true
				break
			}
		}
	}
	if !any {
		// nothing to do here
		return
	}

	var idx *blockfmt.Index
	var err error
	if empty {
		idx = &blockfmt.Index{
			Name: st.table,
			// this is a new index, so we have
			// to respect NewIndexScan configuration
			Scanning: st.shouldScan(),
		}
		st.overwrite(idx, "")
	} else {
		// we re-load the index so that we don't have to
		// worry about reverting any changes we made
		// to the index object
		// (it is expensive to preemptively perform a deep copy)
		idx, err = st.index(ctx)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				idx = &blockfmt.Index{
					Name:     st.table,
					Scanning: st.shouldScan(),
				}
				st.overwrite(idx, "")
			} else {
				return
			}
		}
	}
	idx.Inputs.Backing = st.ofs
	for i := range parts {
		for j := range parts[i].lst {
			if parts[i].lst[j].Err == nil || !blockfmt.IsFatal(parts[i].lst[j].Err) {
				continue
			}
			path := parts[i].lst[j].Path
			etag := parts[i].lst[j].ETag
			err := parts[i].lst[j].Err
			st.logf("rejecting object: path %s etag %s %s", path, etag, err)
			_, err = idx.Inputs.Append(path, etag, -1)
			if err != nil {
				st.logf("blockfmt.FileTree.Append: %s", err)
				return
			}
		}
	}
	idx.Created = date.Now()
	idx.Algo = "zstd"
	err = st.flush(ctx, idx)
	if err != nil {
		st.logf("flushing index in updateFailed: %s", err)
	}
}

// purgeExpired purges expired entries,
// returning a value indicating whether or not
// any entries were expired
func (st *tableState) purgeExpired(idx *blockfmt.Index) bool {
	rp := st.def.Retention
	if rp == nil {
		return false
	}
	if rp.Field == "" {
		st.logf("retention policy field name is not set")
		return false
	}
	if rp.ValidFor.Zero() {
		st.logf("retention policy expiry time is not set or invalid")
		return false
	}
	field, err := expr.ParsePath(rp.Field)
	if err != nil {
		return false
	}
	// field >= (now - validity)
	exp := rp.ValidFor.Sub(date.Now())
	cond := expr.Compare(expr.GreaterEquals, field, &expr.Timestamp{Value: exp})

	var filt blockfmt.Filter // match => keep
	filt.Compile(cond)
	// purge indirect tree
	todelete, err := idx.Indirect.Purge(st.ofs, &filt, st.conf.GCMinimumAge)
	if err != nil {
		st.logf("failed purging expired entries: %s", err)
		return false
	}
	// purge inline list
	expiry := date.Now().Add(st.conf.GCMinimumAge)
	var keep []blockfmt.Descriptor
	for i := range idx.Inline {
		if filt.MatchesAny(&idx.Inline[i].Trailer.Sparse) {
			keep = append(keep, idx.Inline[i])
			continue
		}
		todelete = append(todelete, blockfmt.Quarantined{
			Expiry: expiry,
			Path:   idx.Inline[i].Path,
		})
	}
	idx.Inline = keep
	if len(todelete) == 0 {
		return false
	}
	idx.ToDelete = append(idx.ToDelete, todelete...)
	return true
}

// preciseGC runs precise garbage collection,
// returning a value indicating whether any work
// was done (and thus whether the index should
// be flushed)
func (st *tableState) preciseGC(idx *blockfmt.Index) bool {
	purged := st.purgeExpired(idx)
	gc := false
	if rmfs, ok := st.ofs.(RemoveFS); ok {
		gcconf := GCConfig{Precise: true, Logf: st.logf}
		gc = gcconf.preciseGC(rmfs, idx)
	}
	return purged || gc
}

func (c *Config) maxInlineBytes() int64 {
	if c.MaxInlineBytes <= 0 {
		return DefaultMaxInlineBytes
	}
	return c.MaxInlineBytes
}

func (c *Config) inputMinAge() time.Duration {
	if c.InputMinimumAge <= 0 {
		return DefaultInputMinimumAge
	}
	return c.InputMinimumAge
}

func (st *tableState) addDefHash(d ion.Datum) ion.Datum {
	f := ion.Field{
		Label: "definition",
		Datum: ion.NewStruct(nil, []ion.Field{{
			Label: "hash",
			Datum: ion.Blob(st.def.Hash()),
		}}).Datum(),
	}
	s, err := d.Struct()
	if err != nil {
		return ion.NewStruct(nil, []ion.Field{f}).Datum()
	}
	return s.WithField(f).Datum()
}

func (st *tableState) writeIndex(idx *blockfmt.Index) error {
	idp := IndexPath(st.db, st.table)
	info, err := fs.Stat(st.ofs, idp)
	if st.cache.etag == "" {
		// expect no file to exist
		if err == nil || !errors.Is(err, fs.ErrNotExist) {
			st.invalidate()
			return fmt.Errorf("synchronization violation detected: fs.Stat for %s produced %v", idp, err)
		}
	} else {
		if err != nil {
			return fmt.Errorf("writeIndex: %w", err)
		}
		etag, err := st.ofs.ETag(idp, info)
		if err != nil {
			return fmt.Errorf("writeIndex: determining etag: %w", err)
		}
		if st.cache.etag != etag {
			st.invalidate()
			return fmt.Errorf("synchronization violation detected: found etag %s -> %s", st.cache.etag, etag)
		}
	}
	buf, err := blockfmt.Sign(st.owner.Key(), idx)
	if err != nil {
		return err
	}
	if len(buf) > MaxIndexSize {
		return fmt.Errorf("index would be %d bytes; greater than max %d", len(buf), MaxIndexSize)
	}
	if st.conf.Verbose {
		st.conf.Logf("writing %v bytes to index path %q", len(buf), idp)
	}
	etag, err := st.ofs.WriteFile(idp, buf)
	if err == nil {
		st.overwrite(idx, etag)
	}
	return err
}

// flush writes out the provided index
// and updates or invalidates cache to point
// to the new index value + etag
func (st *tableState) flush(ctx context.Context, idx *blockfmt.Index) (err error) {
	defer func() {
		if err != nil {
			st.invalidate()
		}
	}()

	idx.Name = st.table
	idx.UserData = st.addDefHash(idx.UserData)
	idx.Inputs.Backing = st.ofs
	dir := path.Join("db", st.db, st.table)
	trace.WithRegion(ctx, "flush-inputs", func() {
		err = idx.SyncInputs(dir, st.conf.inputMinAge())
	})
	if err != nil {
		return err
	}
	c := blockfmt.IndexConfig{
		MaxInlined:    st.conf.maxInlineBytes(),
		TargetSize:    int64(st.conf.targetMerge()),
		TargetRefSize: st.conf.TargetRefSize,
		Expiry:        st.conf.GCMinimumAge,
	}
	trace.WithRegion(ctx, "flush-outputs", func() {
		err = c.SyncOutputs(idx, st.ofs, dir)
	})
	if err != nil {
		return err
	}
	err = st.writeIndex(idx)
	return err
}

func suffixForComp(c string) string {
	if c == "zstd" {
		return ".ion.zst"
	}
	if strings.HasPrefix(c, "zion") {
		return ".zion"
	}
	panic("bad suffixForComp value")
	return ""
}

// errUpdateFailed is used to signal that a call
// to forcePart failed during the call to
// (*blockfmt.Converter).Run and the index needs
// to be updated accordingly to reflect that.
type errUpdateFailed struct {
	err error
}

func (e *errUpdateFailed) Error() string {
	return fmt.Sprintf("db.Config: running blockfmt.Converter: %v", e.err)
}

func (e *errUpdateFailed) Unwrap() error {
	return e.err
}

func (st *tableState) force(ctx context.Context, idx *blockfmt.Index, parts []partition) error {
	extra := make([]blockfmt.Descriptor, 0, len(parts))
	errs := make([]error, len(parts))
	var wg sync.WaitGroup
	wg.Add(len(parts))
	for i := range parts {
		var prepend, dst *blockfmt.Descriptor
		if p := parts[i].prepend; p >= 0 {
			prepend = &idx.Inline[p]
			dst = &idx.Inline[p]
		} else {
			extra = extra[:len(extra)+1]
			dst = &extra[len(extra)-1]
		}
		go func(i int) {
			defer wg.Done()
			errs[i] = st.forcePart(ctx, prepend, dst, &parts[i])
		}(i)
	}
	wg.Wait()
	var ferr *errUpdateFailed
	for i := range errs {
		if errs[i] != nil && !errors.As(errs[i], &ferr) {
			return errs[i]
		}
	}
	if ferr != nil {
		st.updateFailed(ctx, idx == nil, parts)
		return ferr
	}
	if idx == nil {
		idx = new(blockfmt.Index)
		for i := range parts {
			for j := range parts[i].lst {
				idx.Inputs.Append(parts[i].lst[j].Path, parts[i].lst[j].ETag, 1)
			}
		}
	}
	idx.Algo = "zstd"
	idx.Created = date.Now().Truncate(time.Microsecond)
	idx.Inline = append(idx.Inline, extra...)
	return st.flush(ctx, idx)
}

func (st *tableState) forcePart(ctx context.Context, prepend, dst *blockfmt.Descriptor, part *partition) error {
	defer trace.StartRegion(ctx, "force-part").End()
	c := blockfmt.Converter{
		Inputs:              part.lst,
		Align:               st.conf.align(),
		FlushMeta:           st.conf.flushMeta(),
		Comp:                st.conf.comp(),
		Constants:           part.cons,
		MinInputBytesPerCPU: st.conf.MinInputBytesPerCPU,
	}

	if prepend != nil {
		f, err := open(st.ofs, prepend.Path, prepend.ETag, prepend.Size)
		if err != nil {
			return fmt.Errorf("opening %s for re-ingest: %w", prepend.Path, err)
		}
		defer f.Close()
		// NOTE: make sure R is an *s3.File here when we're on AWS;
		// that way we can use server-side copy for some prepends
		c.Prepend.R = f
		c.Prepend.Trailer = &prepend.Trailer
	}

	name := "packed-" + uuid() + suffixForComp(c.Comp)
	fp := path.Join("db", st.db, st.table, part.name, name)
	out, err := st.ofs.Create(fp)
	if err != nil {
		return err
	}
	c.Output = out
	err = c.Run()
	if err != nil {
		abort(out)
		return &errUpdateFailed{err: err}
	}
	etag, lastmod, err := getInfo(st.ofs, fp, out)
	if err != nil {
		return err
	}
	*dst = blockfmt.Descriptor{
		ObjectInfo: blockfmt.ObjectInfo{
			Path:         fp,
			LastModified: date.FromTime(lastmod),
			ETag:         etag,
			Format:       blockfmt.Version,
			Size:         out.Size(),
		},
		Trailer: *c.Trailer(),
	}
	return nil
}

func (st *tableState) fullGC(ctx context.Context, idx *blockfmt.Index) error {
	rmfs, ok := st.ofs.(RemoveFS)
	if !ok {
		return nil
	}
	defer trace.StartRegion(ctx, "run-gc").End()
	conf := GCConfig{
		Logf:            st.logf,
		MinimumAge:      st.conf.GCMinimumAge,
		InputMinimumAge: st.conf.InputMinimumAge,
		MaxDelay:        st.conf.GCMaxDelay,
	}
	return conf.Run(rmfs, st.db, idx)
}
