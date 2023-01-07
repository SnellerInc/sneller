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
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"io/fs"
	prand "math/rand"
	"path"
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
const DefaultAlgo = "zion"

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

	// GCLikelihood is the likelihood that
	// a Sync, Append, or Scan operation
	// is followed by a GC operation.
	// This value is interpreted as a statistical
	// percent likelihood, so 0 means never GC,
	// 100 means always GC, and number in beteween
	// mean GC if rand.Intn(100) < GCLikelihood
	GCLikelihood int
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
	def       *Definition
	conf      Config
	owner     Tenant
	ofs       OutputFS
	db, table string
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
		def = &Definition{Name: table}
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

func (st *tableState) index(cache *IndexCache) (*blockfmt.Index, error) {
	if cache != nil && cache.value != nil {
		return cache.value, nil
	}
	ipath := IndexPath(st.db, st.table)
	idx, info, err := openIndex(st.ofs, ipath, st.owner.Key(), 0)
	if err != nil {
		return nil, err
	}
	etag, err := st.ofs.ETag(ipath, info)
	if err != nil {
		return nil, err
	}
	overwrite(cache, idx, etag)
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
	st.conf.logf("re-ingesting %s due to small size", idx.Inline[i].Path)
	idx.ToDelete = append(idx.ToDelete, blockfmt.Quarantined{
		Path:   idx.Inline[i].Path,
		Expiry: date.Now().Add(st.conf.GCMinimumAge),
	})
}

func (st *tableState) dedup(idx *blockfmt.Index, parts []partition) ([]partition, error) {
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
func (ti *tableInfo) append(parts []partition) error {
	idx, err := ti.state.index(&ti.cache)
	if err == nil {
		// begin by removing any unreferenced files,
		// if we have some left around that need removing
		ti.state.preciseGC(idx)
		idx.Inputs.Backing = ti.state.ofs
		if idx.Scanning {
			_, err = ti.state.scan(idx, &ti.cache, true)
			if err != nil {
				return err
			}
			// currently rebuilding; please try again
			invalidate(&ti.cache)
			return ErrBuildAgain
		}
		// trim pre-existing elements from lst
		parts, err = ti.state.dedup(idx, parts)
		if err != nil {
			return err
		}
		if len(parts) == 0 {
			ti.state.conf.logf("index for %s already up-to-date", ti.state.table)
			return nil
		}
		return ti.state.append(idx, parts, &ti.cache)
	}
	if ti.state.shouldScan() && (errors.Is(err, fs.ErrNotExist) || errors.Is(err, blockfmt.ErrIndexObsolete)) {
		idx := &blockfmt.Index{
			Name: ti.state.table,
			Algo: "zstd",
		}
		_, err = ti.state.scan(idx, &ti.cache, true)
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
	return ti.state.append(nil, parts, &ti.cache)
}

func (st *tableState) shouldScan() bool { return st.conf.shouldScan(st.def) }

func (st *tableState) append(idx *blockfmt.Index, parts []partition, cache *IndexCache) error {
	st.conf.logf("updating table %s/%s...", st.db, st.table)
	var err error
	if len(parts) == 0 {
		if idx == nil {
			err = st.emptyIndex(cache)
		}
	} else {
		err = st.force(idx, parts, cache)
	}
	if err != nil {
		invalidate(cache)
		return fmt.Errorf("force: %w", err)
	}
	st.conf.logf("update of table %s complete", st.table)
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
		c.logf("detected table at path %q", tab)
		tables = append(tables, path.Base(tab))
	}
	syncTable := func(table string) error {
		st, err := c.open(db, table, who)
		if err != nil {
			return err
		}
		fresh := false
		gc := false
		idx, err := st.index(nil)
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
		_, err = st.scan(idx, nil, fresh || !restart || gc)
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

func (st *tableState) emptyIndex(cache *IndexCache) error {
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
		overwrite(cache, &idx, etag)
	} else {
		invalidate(cache)
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
func (st *tableState) updateFailed(empty bool, parts []partition, cache *IndexCache) {
	// invalidate cache so that a reload pulls the previous one
	invalidate(cache)

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
		overwrite(cache, idx, "")
	} else {
		// we re-load the index so that we don't have to
		// worry about reverting any changes we made
		// to the index object
		// (it is expensive to preemptively perform a deep copy)
		idx, err = st.index(cache)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				idx = &blockfmt.Index{
					Name:     st.table,
					Scanning: st.shouldScan(),
				}
				overwrite(cache, idx, "")
			} else {
				st.conf.logf("re-opening index to record failure: %s", err)
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
			_, err := idx.Inputs.Append(parts[i].lst[j].Path, parts[i].lst[j].ETag, -1)
			if err != nil {
				st.conf.logf("updateFailed: %s", err)
				return
			}
		}
	}
	idx.Created = date.Now()
	idx.Algo = "zstd"
	err = st.flush(idx, cache)
	if err != nil {
		st.conf.logf("flushing index in updateFailed: %s", err)
	}
}

// purgeExpired purges expired entries,
// returning a value indicating whether or not
// any entries were expired
func (st *tableState) purgeExpired(idx *blockfmt.Index) bool {
	rp := st.def.Retention
	if rp == nil || rp.Field == "" || rp.ValidFor.Zero() {
		return false
	}
	field, err := expr.ParsePath(rp.Field)
	if err != nil {
		return false
	}
	// field >= (now - validity)
	exp := rp.ValidFor.Sub(date.Now())
	cond := expr.Compare(expr.GreaterEquals, field, &expr.Timestamp{Value: exp})

	var filt blockfmt.Filter
	filt.Compile(cond)
	todelete, err := idx.Indirect.Purge(st.ofs, &filt, st.conf.GCMinimumAge)
	if err != nil {
		st.conf.logf("failed purging expired entries: %s", err)
		return false
	}
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
	if rmfs, ok := st.ofs.(RemoveFS); ok && st.conf.GCLikelihood > 0 {
		gcconf := GCConfig{Precise: true, Logf: st.conf.Logf}
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

// userdata makes a datum to place into the
// userdata field of the index.
func (st *tableState) userdata() ion.Datum {
	return ion.NewStruct(nil, []ion.Field{{
		Label: "definition",
		Value: ion.NewStruct(nil, []ion.Field{{
			Label: "hash",
			Value: ion.Blob(st.def.Hash()),
		}}).Datum(),
	}}).Datum()
}

func (st *tableState) writeIndex(idx *blockfmt.Index, cache *IndexCache) error {
	idp := IndexPath(st.db, st.table)
	if cache != nil {
		info, err := fs.Stat(st.ofs, idp)
		if cache.etag == "" {
			// expect no file to exist
			if err == nil || !errors.Is(err, fs.ErrNotExist) {
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
			if cache.etag != etag {
				return fmt.Errorf("synchronization violation detected: found etag %s -> %s", cache.etag, etag)
			}
		}
	}
	buf, err := blockfmt.Sign(st.owner.Key(), idx)
	if err != nil {
		return err
	}
	if len(buf) > MaxIndexSize {
		return fmt.Errorf("index would be %d bytes; greater than max %d", len(buf), MaxIndexSize)
	}
	etag, err := st.ofs.WriteFile(idp, buf)
	if err == nil {
		overwrite(cache, idx, etag)
	}
	return err
}

// flush writes out the provided index
// and updates or invalidates cache to point
// to the new index value + etag
func (st *tableState) flush(idx *blockfmt.Index, cache *IndexCache) (err error) {
	idx.Name = st.table
	idx.UserData = st.userdata()
	idx.Inputs.Backing = st.ofs
	dir := path.Join("db", st.db, st.table)
	err = idx.SyncInputs(dir, st.conf.inputMinAge())
	if err != nil {
		invalidate(cache)
		return err
	}
	c := blockfmt.IndexConfig{
		MaxInlined:    st.conf.maxInlineBytes(),
		TargetSize:    int64(st.conf.targetMerge()),
		TargetRefSize: st.conf.TargetRefSize,
		Expiry:        st.conf.GCMinimumAge,
	}
	err = c.SyncOutputs(idx, st.ofs, dir)
	if err != nil {
		invalidate(cache)
		return err
	}
	err = st.writeIndex(idx, cache)
	if err != nil {
		invalidate(cache)
		return err
	}
	return nil
}

func suffixForComp(c string) string {
	switch c {
	case "zstd":
		return ".ion.zst"
	case "zion":
		return ".zion"
	default:
		panic("bad suffixForComp value")
	}
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

func (st *tableState) force(idx *blockfmt.Index, parts []partition, cache *IndexCache) error {
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
			errs[i] = st.forcePart(prepend, dst, &parts[i])
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
		st.updateFailed(idx == nil, parts, cache)
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
	err := st.flush(idx, cache)
	if err != nil {
		return err
	}
	return st.runGC(idx)
}

func (st *tableState) forcePart(prepend, dst *blockfmt.Descriptor, part *partition) error {
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
	st.conf.logf("table %s: wrote object %s ETag %s", st.table, fp, etag)
	return nil
}

func (st *tableState) runGC(idx *blockfmt.Index) error {
	if prand.Intn(100) >= st.conf.GCLikelihood {
		return nil
	}
	rmfs, ok := st.ofs.(RemoveFS)
	if !ok {
		return nil
	}
	conf := GCConfig{
		Logf:            st.conf.Logf,
		MinimumAge:      st.conf.GCMinimumAge,
		InputMinimumAge: st.conf.InputMinimumAge,
	}
	return conf.Run(rmfs, st.db, idx)
}
