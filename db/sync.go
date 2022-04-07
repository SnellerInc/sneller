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
	"io"
	"io/fs"
	prand "math/rand"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/SnellerInc/sneller/date"
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

// DefaultRangeMultiple is the default
// multiple of the chunk alignment at which
// we write out metadata.
const DefaultRangeMultiple = 100

// ErrBuildAgain is returned by db.Builder.Sync
// when only some of the input objects were successfully
// ingested.
var ErrBuildAgain = errors.New("partial db update")

// Builder is a set of configuration items
// for synchronizing an Index to match
// a specification from a Definition.
type Builder struct {
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
	// Force forces a full index rebuild
	// even when the input appears to be up-to-date.
	Force bool
	// Fallback determines the format for
	// objects when the object format is not
	// obvious from the file extension.
	Fallback func(name string) blockfmt.RowFormat
	// MaxInputBytes is the maximum
	// number of input bytes read into the table
	// before ErrBuildAgain is returned.
	// If MaxInputBytes is less than or equal to
	// zero, it is ignored.
	MaxInputBytes int64
	// MaxScanObjects is the maximum number
	// of objects to be committed in a single Scan operation.
	// If MaxScanObjects is less than or equal to zero,
	// it is ignored and no limit is applied.
	MaxScanObjects int
	// MaxScanBytes is the maximum number
	// of bytes to ingest in a single Scan operation
	// (not including merging).
	// If MaxScanBytes is less than or equal to zero,
	// it is ignored and no limit is applied.
	MaxScanBytes int64
	// NewIndexScan, if true, enables scanning
	// for newly-created index objects.
	NewIndexScan bool

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
	// it has been dereferenced. If this is set
	// to zero, then the default is 15 minutes.
	GCMinimumAge time.Duration

	// Logf, if non-nil, will be where
	// the builder will log build actions
	// as it is executing. Logf must be
	// safe to call from multiple goroutines
	// simultaneously.
	Logf func(f string, args ...interface{})
}

// Format picks the row format for an object
// based on an explicit format hint and the object name.
// The following are tried, in order:
//   1. If 'chosen' is the name of a known format,
//      then that format is returned.
//   2. If 'name' has a suffix that indicates a known format,
//      then that format is returned.
//   3. If b.Fallback is non-nil, then Fallback(name) is returned.
// Otherwise, Format returns nil.
func (b *Builder) Format(chosen, name string) blockfmt.RowFormat {
	if chosen != "" {
		if f := blockfmt.SuffixToFormat["."+chosen]; f != nil {
			return f()
		}
	}
	for suff, f := range blockfmt.SuffixToFormat {
		if strings.HasSuffix(name, suff) {
			return f()
		}
	}
	if b.Fallback != nil {
		return b.Fallback(name)
	}
	return nil
}

func (b *Builder) logf(f string, args ...interface{}) {
	if b.Logf != nil {
		b.Logf(f, args...)
	}
}

type tableState struct {
	conf      *Builder
	owner     Tenant
	ofs       OutputFS
	db, table string
}

func (b *Builder) open(db, table string, owner Tenant) (*tableState, error) {
	ifs, err := owner.Root()
	if err != nil {
		return nil, err
	}
	ofs, ok := ifs.(OutputFS)
	if !ok {
		return nil, fmt.Errorf("root %T is read-only", ifs)
	}
	return &tableState{
		conf:  b,
		owner: owner,
		ofs:   ofs,
		db:    db,
		table: table,
	}, nil
}

func (st *tableState) index() (*blockfmt.Index, error) {
	return OpenIndex(st.ofs, st.db, st.table, st.owner.Key())
}

func (st *tableState) def() (*Definition, error) {
	return OpenDefinition(st.ofs, st.db, st.table)
}

var (
	// ErrDuplicateObject occurs when an object
	// collected as part of a Definition shares an ETag
	// with another object in that Definition.
	// (We insist that the set of objects written
	// into an Index be unique.)
	ErrDuplicateObject = errors.New("duplicate input object")
)

// constrain the combined size of lst
// to b.MaxInputBytes as long as it is set
func (b *Builder) maxTrim(lst []blockfmt.Input) ([]blockfmt.Input, bool) {
	if b.MaxInputBytes <= 0 {
		return lst, false
	}
	sum := int64(0)
	for i := range lst {
		sum += lst[i].Size
		// always accept the first object, even
		// if it is larger than MaxInputBytes, just
		// to guarantee that we make forward progress
		if sum >= b.MaxInputBytes && i > 0 {
			b.logf("constraining input to %d of %d items", i, len(lst))
			tail := lst[i:]
			for j := range tail {
				tail[j].R.Close()
			}
			return lst[:i], true
		}
	}
	return lst, false
}

func (b *Builder) popPrepend(idx *blockfmt.Index) *blockfmt.Descriptor {
	l := len(idx.Contents)
	if l > 0 && idx.Contents[l-1].Size < b.minMergeSize() {
		ret := &idx.Contents[l-1]
		idx.Contents = idx.Contents[:l-1]
		return ret
	}
	return nil
}

// if 'lst' consists of a superset of the contents of idx,
// then filter out all the members of lst that are already
// present and return the trimmed list of inputs
func (b *Builder) isAppend(idx *blockfmt.Index, lst []blockfmt.Input) (*blockfmt.Descriptor, []blockfmt.Input, bool) {
	prepend := b.popPrepend(idx)
	descID := len(idx.Contents)
	var kept []blockfmt.Input
	for i := range lst {
		ret, err := idx.Inputs.Append(lst[i].Path, lst[i].ETag, descID)
		if err != nil {
			if !errors.Is(err, blockfmt.ErrETagChanged) {
				b.logf("blockfmt.FileTree.Append: %s", err)
			}
			// FIXME: handle ErrETagChanged
			// differently from an I/O error
			return nil, nil, false
		}
		if ret {
			kept = append(kept, lst[i])
		} else {
			lst[i].R.Close()
		}
	}
	if len(kept) == 0 {
		return nil, nil, true
	}
	return prepend, kept, true
}

func (st *tableState) dedup(idx *blockfmt.Index, lst []blockfmt.Input) (*blockfmt.Descriptor, []blockfmt.Input, error) {
	prepend := st.conf.popPrepend(idx)
	descID := len(idx.Contents)
	var kept []blockfmt.Input
	for i := range lst {
		ret, err := idx.Inputs.Append(lst[i].Path, lst[i].ETag, descID)
		if err != nil {
			return nil, nil, err
		}
		if ret {
			kept = append(kept, lst[i])
		} else {
			lst[i].R.Close()
		}
	}
	if len(kept) == 0 {
		// nothing new to do; just bail
		return nil, nil, nil
	}
	if prepend != nil {
		st.conf.logf("re-ingesting %s due to small size", prepend.Path)
	}
	return prepend, kept, nil
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

// Append works similarly to Sync,
// but it only works on one database and table at a time,
// and it assumes the list of new elements to be inserted
// has already been computed.
//
// If the index to be updated is currently scanning (see Builder.Scan),
// then Append will perform some scanning inserts and return ErrBuildAgain.
// Append will continue to return ErrBuildAgain until scanning is complete,
// at which point Append operations will be accepted. (The caller must continuously
// call Append for scanning to occur.)
func (b *Builder) Append(who Tenant, db, table string, lst []blockfmt.Input) error {
	st, err := b.open(db, table, who)
	if err != nil {
		return err
	}

	var prepend *blockfmt.Descriptor
	idx, err := st.index()
	if err == nil {
		idx.Inputs.Backing = st.ofs
		if idx.Scanning {
			def, err := st.def()
			if err != nil {
				return err
			}
			_, err = st.scan(def, idx)
			if err != nil {
				return err
			}
			// currently rebuilding; please try again
			return ErrBuildAgain
		}

		// trim pre-existing elements from
		prepend, lst, err = st.dedup(idx, lst)
		if err != nil {
			return err
		}
		if len(lst) == 0 {
			b.logf("index for %s already up-to-date", table)
			return nil
		}
		return st.append(idx, prepend, lst)
	}
	if b.NewIndexScan && (errors.Is(err, fs.ErrNotExist) || errors.Is(err, blockfmt.ErrIndexObsolete)) {
		def, err := OpenDefinition(st.ofs, db, table)
		if err != nil {
			return err
		}
		idx := &blockfmt.Index{
			Name: table,
			Algo: "zstd",
		}
		_, err = st.scan(def, idx)
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
	return st.append(nil, prepend, lst)
}

func (st *tableState) append(idx *blockfmt.Index, prepend *blockfmt.Descriptor, tail []blockfmt.Input) error {
	lst, partial := st.conf.maxTrim(tail)
	st.conf.logf("updating table %s/%s...", st.db, st.table)
	var err error
	if len(lst) == 0 {
		err = st.emptyIndex()
	} else {
		err = st.force(idx, prepend, lst)
	}
	if err != nil {
		return fmt.Errorf("force: %w", err)
	}
	if partial {
		st.conf.logf("update of table %s partially complete", st.table)
		return ErrBuildAgain
	}
	st.conf.logf("update of table %s complete", st.table)
	return nil
}

// Sync reads each Definition in dst,
// converts the list of input objects
// into the right set of output objects,
// and writes the associated index signed with 'key'.
func (b *Builder) Sync(who Tenant, db, tblpat string) error {
	if tblpat == "" {
		tblpat = "*"
	}
	dst, err := who.Root()
	if err != nil {
		return err
	}
	possible, err := fs.Glob(dst, path.Join("db", db, tblpat, "definition.[yj][sa][om][nl]"))
	if err != nil {
		return err
	}
	var tables []string
	for i := range possible {
		switch path.Base(possible[i]) {
		case "definition.json", "definition.yaml":
			tab, _ := path.Split(possible[i])
			b.logf("detected table at path %q", tab)
			tables = append(tables, path.Base(tab))
		default:
			// continue
		}
	}

	syncTable := func(table string) error {
		st, err := b.open(db, table, who)
		if err != nil {
			return err
		}
		s, err := st.def()
		if err != nil {
			return err
		}
		lst, err := s.resolve(b, who)
		if err != nil {
			b.logf("resolving inputs for %s: %s", table, err)
			return fmt.Errorf("resolving table %s: %w", table, err)
		}
		if len(lst) == 0 {
			b.logf("warning: table %s is empty", table)
		}
		if !b.Force {
			idx, err := st.index()
			if err == nil && idx.Name == table {
				idx.Inputs.Backing = st.ofs
				prepend, tail, ok := b.isAppend(idx, lst)
				if ok {
					if len(tail) == 0 {
						b.logf("table %s up-to-date", table)
						return nil
					}
					return st.append(idx, prepend, tail)
				}
			}
			if !shouldRebuild(err) {
				return err
			}
		}
		return st.append(nil, nil, lst)
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

// resolve a definition to a list of inputs using a Resolver
func (s *Definition) resolve(b *Builder, who Tenant) ([]blockfmt.Input, error) {
	var out []blockfmt.Input
	for i := range s.Inputs {
		infs, pat, err := who.Split(s.Inputs[i].Pattern)
		if err != nil {
			return nil, err
		}
		fallback := func(name string) blockfmt.RowFormat {
			return b.Format(s.Inputs[i].Format, name)
		}
		in, err := blockfmt.CollectGlob(infs, fallback, pat)
		if err != nil {
			return nil, err
		}
		out = append(out, in...)
	}
	return out, nil
}

func (b *Builder) align() int {
	if b.Align > 0 {
		return b.Align
	}
	return 1024 * 1024
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
		Created: date.Now().Truncate(time.Microsecond),
		Name:    st.table,
		// no Contents
	}
	buf, err := blockfmt.Sign(st.owner.Key(), &idx)
	if err != nil {
		return err
	}
	p := IndexPath(st.db, st.table)
	_, err = st.ofs.WriteFile(p, buf)
	return err
}

func (b *Builder) flushMeta() int {
	align := b.align()
	if b.RangeMultiple <= 0 {
		return align * DefaultRangeMultiple
	}
	return align * b.RangeMultiple
}

// after failing to read an object,
// update the index state to reflect the fatal errors we encountered
func (st *tableState) updateFailed(empty bool, lst []blockfmt.Input) {
	any := false
	for i := range lst {
		if lst[i].Err != nil && blockfmt.IsFatal(lst[i].Err) {
			any = true
			break
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
		}
	} else {
		// we re-load the index so that we don't have to
		// worry about reverting any changes we made
		// to the index object
		// (it is expensive to preemptively perform a deep copy)
		idx, err = st.index()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				idx = &blockfmt.Index{Name: st.table}
			} else {
				st.conf.logf("re-opening index to record failure: %s", err)
				return
			}
		}
	}
	idx.Inputs.Backing = st.ofs
	for i := range lst {
		if lst[i].Err == nil || !blockfmt.IsFatal(lst[i].Err) {
			continue
		}
		_, err := idx.Inputs.Append(lst[i].Path, lst[i].ETag, -1)
		if err != nil {
			st.conf.logf("updateFailed: Append: %s", err)
			return
		}
	}
	idx.Created = date.Now()
	idx.Algo = "zstd"
	err = st.flush(idx)
	if err != nil {
		st.conf.logf("flushing index in updateFailed: %s", err)
	}
}

type readCloser struct {
	io.Reader
	io.Closer
}

func (st *tableState) flush(idx *blockfmt.Index) error {
	idx.Name = st.table
	idx.Inputs.Backing = st.ofs
	err := idx.SyncInputs(path.Join("db", st.db, st.table))
	if err != nil {
		return err
	}
	buf, err := blockfmt.Sign(st.owner.Key(), idx)
	if err != nil {
		return err
	}
	if len(buf) > MaxIndexSize {
		return fmt.Errorf("index would be %d bytes; greater than max %d", len(buf), MaxIndexSize)
	}
	idp := IndexPath(st.db, st.table)
	_, err = st.ofs.WriteFile(idp, buf)
	return err
}

func (st *tableState) force(idx *blockfmt.Index, prepend *blockfmt.Descriptor, lst []blockfmt.Input) error {
	c := blockfmt.Converter{
		Inputs:    lst,
		Align:     st.conf.align(),
		FlushMeta: st.conf.flushMeta(),
		Comp:      "zstd",
	}

	if prepend != nil {
		f, err := st.ofs.Open(prepend.Path)
		if err != nil {
			return fmt.Errorf("opening %s for re-ingest: %w", prepend.Path, err)
		}
		info, err := f.Stat()
		if err != nil {
			return fmt.Errorf("stat-ing re-ingest descriptor: %w", err)
		}
		etag, err := st.ofs.ETag(prepend.Path, info)
		if err != nil {
			return fmt.Errorf("getting ETag: %w", err)
		}
		if etag != prepend.ETag {
			return fmt.Errorf("ETag has changed: %s -> %s", prepend.ETag, etag)
		}
		tr := prepend.Trailer
		c.Prepend.R = &readCloser{Reader: io.LimitReader(f, tr.Offset), Closer: f}
		c.Prepend.Trailer = tr
	}

	name := "packed-" + uuid() + ".ion.zst"
	fp := path.Join("db", st.db, st.table, name)
	out, err := st.ofs.Create(fp)
	if err != nil {
		return err
	}
	c.Output = out
	err = c.Run()
	if err != nil {
		abort(out)
		if c.Prepend.R != nil {
			// re-extend Contents (see popPrepend())
			idx.Contents = idx.Contents[:len(idx.Contents)+1]
			if prepend != &idx.Contents[len(idx.Contents)-1] {
				panic("could not re-extend contents")
			}
		}
		st.updateFailed(idx == nil, c.Inputs)
		return fmt.Errorf("db.Builder: running blockfmt.Converter: %w", err)
	}
	etag, lastmod, err := getInfo(st.ofs, fp, out)
	if err != nil {
		return err
	}
	st.conf.logf("table %s: wrote object %s ETag %s", st.table, fp, etag)
	buildtime := date.Now().Truncate(time.Microsecond)
	if idx == nil {
		idx = new(blockfmt.Index)
		for i := range lst {
			idx.Inputs.Append(lst[i].Path, lst[i].ETag, 0)
		}
	}
	idx.Algo = "zstd"
	idx.Created = buildtime
	idx.Contents = append(idx.Contents, blockfmt.Descriptor{
		ObjectInfo: blockfmt.ObjectInfo{
			Path:         fp,
			LastModified: date.FromTime(lastmod),
			ETag:         etag,
			Format:       blockfmt.Version,
			Size:         out.Size(),
		},
		Trailer: c.Trailer(),
	})
	err = st.flush(idx)
	if err == nil {
		err = st.runGC(idx)
	}
	return err
}

func (st *tableState) runGC(idx *blockfmt.Index) error {
	if prand.Intn(100) >= st.conf.GCLikelihood {
		return nil
	}
	rmfs, ok := st.ofs.(RemoveFS)
	if !ok {
		return nil
	}
	conf := GCConfig{Logf: st.conf.Logf, MinimumAge: st.conf.GCMinimumAge}
	return conf.Run(rmfs, st.db, idx)
}
