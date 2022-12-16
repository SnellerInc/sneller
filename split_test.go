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

package sneller

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/date"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
	"github.com/SnellerInc/sneller/plan"
)

func init() {
	plan.AddTransportDecoder("fake-transport", &fakeTransportDecoder{})
}

type fakeTransport string

type fakeTransportDecoder struct {
	transport fakeTransport
}

func (d *fakeTransportDecoder) GetTransport() (plan.Transport, error) {
	return d.transport, nil
}

func (d *fakeTransportDecoder) Init(*ion.Symtab) {}

func (d *fakeTransportDecoder) Finalize() error { return nil }

func (d *fakeTransportDecoder) SetField(name string, body []byte) error {
	if name == "transport" {
		s, _, err := ion.ReadString(body)
		if err != nil {
			return err
		}

		d.transport = fakeTransport(s)
		return nil
	}
	return errors.New("no transport field")
}

func (t fakeTransport) Exec(*plan.Tree, *plan.ExecParams) error {
	panic("fake transport cannot exec")
}

func (t fakeTransport) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("type"))
	dst.WriteSymbol(st.Intern("fake-transport"))
	dst.BeginField(st.Intern("transport"))
	dst.WriteString(string(t))
	dst.EndStruct()
}

// randblobs produces a []int with values chosen from
// the half-open interval [0, n) with probability p.
func randblobs(n int, p float64) []int {
	var blobs []int
	for i := 0; i < n; i++ {
		if rand.Float64() < p {
			blobs = append(blobs, i)
		}
	}
	return blobs
}

func mkblob(url string) blob.Interface {
	now := date.Now().Truncate(time.Microsecond)
	hash := md5.Sum([]byte(url))
	from := &blob.URL{
		Value: url,
		Info: blob.Info{
			ETag:         hex.EncodeToString(hash[:]),
			Size:         12345,
			Align:        1024,
			LastModified: now,
		},
	}
	ranges := make([]blockfmt.Range, 3)
	for i := range ranges {
		path := []string{"foo", fmt.Sprintf("bar%d", i)}
		min := now.Add(time.Duration(-20*i) * time.Hour)
		max := now.Add(time.Duration(-10*i) * time.Hour)
		ranges[i] = blockfmt.NewRange(path, ion.Timestamp(min), ion.Timestamp(max))
	}
	blocks := make([]blockfmt.Blockdesc, 100)
	for i := range blocks {
		blocks[i] = blockfmt.Blockdesc{
			Offset: int64(1000 * i),
			Chunks: 100,
		}
	}
	trailer := &blockfmt.Trailer{
		Version:    1,
		Offset:     12000,
		Algo:       "zstd",
		BlockShift: 20,
		Blocks:     blocks,
	}
	for range blocks {
		// FIXME: these ranges are identical,
		// so they get compressed way too well...
		trailer.Sparse.Push(ranges)
	}
	return &blob.Compressed{
		From:    from,
		Trailer: trailer,
	}
}

func mksubs0(blobs, splits int) plan.Subtables {
	words := []string{"foo", "bar", "baz", "quux"}
	s := make([]split, splits)
	for i := range s {
		s[i].tp = fakeTransport(words[i%len(words)])
		s[i].blobs = randblobs(blobs, 0.1)
	}
	t := expr.String(words[rand.Intn(len(words))])
	b := make([]blob.Interface, blobs)
	for i := range b {
		b[i] = mkblob("https://example.com/blobs/" + words[i%len(words)])
	}
	f := expr.Is(expr.Identifier("foo"), expr.IsNull)
	return &Subtables{
		splits: s,
		table:  t,
		blobs:  b,
		filter: f,
		fn:     blobsToHandle,
	}
}

func mksubs(subs, blobs, splits int) plan.Subtables {
	s := mksubs0(blobs, splits)
	for i := 1; i < subs; i++ {
		s = s.Append(mksubs0(blobs, splits))
	}
	return s
}

func TestSubtables(t *testing.T) {
	want := mksubs(3, 10, 100)
	var buf ion.Buffer
	var st ion.Symtab
	err := want.Encode(&st, &buf)
	if err != nil {
		t.Fatal("encoding:", err)
	}
	var tenv TenantEnv
	got, err := tenv.DecodeSubtables(&st, buf.Bytes())
	if err != nil {
		t.Fatal("decoding:", err)
	}
	if err := subsequal(want, got); err != nil {
		t.Fatal("mismatch after decode:", err)
	}
}

func TestSubtablesSize(t *testing.T) {
	if !testing.Verbose() {
		t.Skip("test requires -v")
	}
	cases := []struct {
		subs, blobs, splits int
	}{
		{1, 3, 10},
		{2, 5, 20},
		{3, 10, 100},
		{4, 20, 200},
		{5, 100, 1000},
	}
	for _, c := range cases {
		subs, blobs, splits := c.subs, c.blobs, c.splits
		t.Run(fmt.Sprintf("%d-%d-%d", subs, blobs, splits), func(t *testing.T) {
			s := mksubs(subs, blobs, splits)
			var buf ion.Buffer
			var st ion.Symtab
			err := s.Encode(&st, &buf)
			if err != nil {
				t.Fatal("encoding:", err)
			}
			size := naivesize(s)
			t.Log("encoded size:", buf.Size())
			t.Log("naive size:  ", size)
			t.Log("ratio:       ", float64(buf.Size())/float64(size))
		})
	}
}

// calculate the size of subtables encoded as a
// subtable list
func naivesize(subs plan.Subtables) int {
	lst := make(plan.SubtableList, subs.Len())
	for i := range lst {
		subs.Subtable(i, &lst[i])
	}
	var st ion.Symtab
	var buf ion.Buffer
	err := lst.Encode(&st, &buf)
	if err != nil {
		panic(err)
	}
	return buf.Size()
}

func subsequal(want, got plan.Subtables) error {
	s1, ok := want.(*Subtables)
	if !ok {
		return fmt.Errorf("want is not type *subtables")
	}
	s2, ok := got.(*Subtables)
	if !ok {
		return fmt.Errorf("got is not type *subtables")
	}
	return subsequaln(0, s1, s2)
}

func subsequaln(n int, s1, s2 *Subtables) error {
	if !reflect.DeepEqual(s1.splits, s2.splits) {
		return fmt.Errorf("sub %d: splits are not equal", n)
	}
	if !reflect.DeepEqual(s1.table, s2.table) {
		return fmt.Errorf("sub %d: tables are not equal", n)
	}
	if want, got := len(s1.blobs), len(s2.blobs); want != got {
		return fmt.Errorf("sub %d: len(blobs) mismatch: %d != %d", n, want, got)
	}
	for i := range s1.blobs {
		if !reflect.DeepEqual(s1.blobs[i], s2.blobs[i]) {
			return fmt.Errorf("sub %d: blobs[%d] are not equal", n, i)
		}
	}
	if !reflect.DeepEqual(s1.filter, s2.filter) {
		return fmt.Errorf("sub %d: filters are not equal", n)
	}
	if s1.next != nil {
		if s2.next == nil {
			return fmt.Errorf("sub %d: want next, got nil next", n)
		}
		return subsequaln(n+1, s1.next, s2.next)
	}
	if s2.next != nil {
		return fmt.Errorf("sub %d: want nil next, got next", n)
	}
	return nil
}
