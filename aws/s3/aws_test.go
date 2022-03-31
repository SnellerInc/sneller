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

package s3

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SnellerInc/sneller/aws"
	"github.com/SnellerInc/sneller/fsutil"
)

// If you have AWS credentials available
// and a test bucket set up, you can run
// this "integration test"
func TestAWS(t *testing.T) {
	bucket := os.Getenv("AWS_TEST_BUCKET")
	if testing.Short() || bucket == "" {
		t.Skip("skipping AWS-specific test")
	}
	rand.Seed(time.Now().Unix())
	prefix := fmt.Sprintf("go-test-%d", rand.Int())
	key, err := aws.AmbientKey("s3", DeriveForBucket(bucket))
	if err != nil {
		t.Skipf("skipping; couldn't derive key: %s", err)
	}
	b := &BucketFS{
		Key:      key,
		Bucket:   bucket,
		DelayGet: true,
	}

	tests := []struct {
		name string
		run  func(t *testing.T, b *BucketFS, prefix string)
	}{
		{
			"BasicCRUD",
			testBasicCrud,
		},
		{
			"WalkGlob",
			testWalkGlob,
		},
	}
	for _, tr := range tests {
		t.Run(tr.name, func(t *testing.T) {
			tr.run(t, b, prefix)
		})
	}

	rm := func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == prefix && errors.Is(err, fs.ErrNotExist) {
				// everthing already cleaned up
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil // cannot rm
		}
		t.Logf("remove %s", p)
		return b.Remove(p)
	}
	// remove everything left under the prefix
	err = fs.WalkDir(b, prefix, rm)
	if err != nil {
		t.Fatalf("removing left-over items: %s", err)
	}
}

// write an object, read it back, and delete it
func testBasicCrud(t *testing.T, b *BucketFS, prefix string) {
	contents := []byte("here are some object contents")
	fullp := path.Join(prefix, "foo/bar/contents")
	etag, err := b.Put(fullp, contents)
	if err != nil {
		t.Fatal(err)
	}
	f, err := b.Open(fullp)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s3f, ok := f.(*File)
	if !ok {
		t.Fatalf("Open returned %T", f)
	}
	if s3f.ETag != etag {
		t.Errorf("returned etag %s expected %s", s3f.ETag, etag)
	}
	if s3f.Path() != fullp {
		t.Errorf("returned path %q expected %q", s3f.Path(), fullp)
	}
	mem, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("reading contents: %s", err)
	}
	if !bytes.Equal(mem, contents) {
		t.Fatalf("got contents %q wanted %q", mem, contents)
	}
	err = f.Close()
	if err != nil {
		t.Errorf("Close: %s", err)
	}
	err = b.Remove(s3f.Path())
	if err != nil {
		t.Fatal(err)
	}

	// should get fs.ErrNotExist on another get
	f, err = b.Open(fullp)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("open non-existent file yields %v", err)
		if f != nil {
			f.Close()
		}
	}
}

func testWalkGlob(t *testing.T, b *BucketFS, prefix string) {
	dirs := []string{
		"a/b/c",
		"x/b/c",
		"x/y/a",
		"x/y/z",
	}
	cases := []struct {
		seek, pattern string
		results       []string
	}{
		{"", "x/?/?", []string{"x/b/c", "x/y/a", "x/y/z"}},
		{"x/y", "?/?/?", []string{"x/y/a", "x/y/z"}},
		{"x/y", "x/*y/*", []string{"x/y/a", "x/y/z"}},
		{"", "x/[by]/c", []string{"x/b/c"}},
		{"x/y/a", "?/?/?", []string{"x/y/z"}},
	}
	for _, full := range dirs {
		full = path.Join(prefix, full)
		_, err := b.Put(full, []byte(fmt.Sprintf("contents of %q", full)))
		if err != nil {
			t.Fatal(err)
		}
	}
	dir, err := b.Open(prefix)
	if err != nil {
		t.Fatal(err)
	}
	pre, ok := dir.(*Prefix)
	if !ok {
		t.Fatalf("got %T back from BucketFS.Open(%s)", dir, prefix)
	}
	for i := range cases {
		seek := cases[i].seek
		pattern := cases[i].pattern
		want := cases[i].results
		var got []string
		err := fsutil.WalkGlob(pre, seek, pattern, func(p string, f fs.File, err error) error {
			if err != nil {
				t.Fatal(err)
			}
			f.Close()
			p = strings.TrimPrefix(p, prefix+"/")
			got = append(got, p)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Errorf("case %d want %v got %v", i, want, got)
		}
	}
}
