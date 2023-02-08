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
	"context"
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
	r := rand.New(rand.NewSource(time.Now().Unix()))
	prefix := fmt.Sprintf("go-test-%d", r.Int())
	key, err := aws.AmbientKey("s3", DeriveForBucket(bucket))
	if err != nil {
		t.Skipf("skipping; couldn't derive key: %s", err)
	}
	b := &BucketFS{
		Key:      key,
		Bucket:   bucket,
		DelayGet: true,
		Ctx:      context.Background(),
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
		{
			"WalkGlobRoot",
			testWalkGlobRoot,
		},
		{
			"ReadDir",
			testReadDir,
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

func testReadDir(t *testing.T, b *BucketFS, prefix string) {
	fullp := path.Join(prefix, "xyz-does-not-exist")
	items, err := fs.ReadDir(b, fullp)
	if len(items) > 0 {
		t.Errorf("got %d items?", len(items))
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("got error %v %[1]T", err)
	}
}

// write an object, read it back, and delete it
func testBasicCrud(t *testing.T, b *BucketFS, prefix string) {
	contents := []byte("here are some object contents")
	fullp := path.Join(prefix, "foo/bar/filename-with:chars= space")
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

	// should get fs.ErrNotExist on another get;
	// this path exercises the list-for-get path:
	f, err = b.Open(fullp)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("open non-existent file yields %v", err)
		if f != nil {
			f.Close()
		}
	}
}

func testWalkGlob(t *testing.T, b *BucketFS, prefix string) {
	// dirs to create; create some placeholder
	// dirs with bad names to test that those are
	// ignored in listing
	dirs := []string{
		"a/b/c",
		"x/", // ignored
		"x/b/c",
		"x/y/",  // ignored
		"x/y/.", // ignored
		"x/y/a",
		"x/y/z",
		"y/",      // ignored
		"y/bc/..", // ignored
		"y/bc/a",
		"y/bc/b",
		"z/#.txt", // exercises sorting
		"z/#/b",
	}
	cases := []struct {
		seek, pattern string
		results       []string
	}{
		{"", "x/?/?", []string{"x/b/c", "x/y/a", "x/y/z"}},
		{"x/y", "?/?/?", []string{"x/y/a", "x/y/z", "z/#/b"}},
		{"x/y", "x/*y/*", []string{"x/y/a", "x/y/z"}},
		{"", "x/[by]/c", []string{"x/b/c"}},
		{"x/y/a", "?/?/?", []string{"x/y/z", "z/#/b"}},
		{"", "?/b*/?", []string{"a/b/c", "x/b/c", "y/bc/a", "y/bc/b"}},
		{"", "z/#*", []string{"z/#.txt"}},
	}
	for _, full := range dirs {
		// NOTE: don't use path.Join, it will remove
		// the trailing '/'
		if prefix[len(prefix)-1] != '/' {
			full = prefix + "/" + full
		} else {
			full = prefix + full
		}
		_, err := b.put(full, []byte(fmt.Sprintf("contents of %q", full)))
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
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			var got []string
			err := fsutil.WalkGlob(pre, seek, pattern, func(p string, f fs.File, err error) error {
				if err != nil {
					t.Errorf("%s: %v\n", p, err)
					return nil
				}
				f.Close()
				p = strings.TrimPrefix(p, prefix+"/")
				got = append(got, p)
				return nil
			})
			if err != nil {
				t.Fatal("fsutil.WalkGlob:", err)
			}
			if !reflect.DeepEqual(want, got) {
				t.Errorf("want %v got %v", want, got)
			}
		})
	}
}

func testWalkGlobRoot(t *testing.T, b *BucketFS, prefix string) {
	name := prefix + ".txt"
	_, err := b.put(name, nil)
	if err != nil {
		t.Fatal("creating test file:", err)
	}
	t.Cleanup(func() {
		t.Log("remove", name)
		b.Remove(name)
	})
	root, err := b.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	pre, ok := root.(*Prefix)
	if !ok {
		t.Fatalf(`got %T back from BucketFS.Open(".")`, root)
	}
	// look for the prefix in the root
	found := false
	err = fsutil.WalkGlob(pre, "", "go-test-*.txt", func(p string, f fs.File, err error) error {
		t.Log("visiting:", p)
		if err != nil {
			t.Errorf("%s: %v\n", p, err)
			return nil
		}
		f.Close()
		if p == name {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatal("fsutil.WalkGlob:", err)
	}
	if !found {
		t.Errorf("could not find %q in the bucket", name)
	}
}

// This test will walk a bucket until we've seen
// 100 files and measure the time taken. It is
// intended to benchmark listing more so than
// test functionality.
func TestAWSListPerformance(t *testing.T) {
	bucket := os.Getenv("AWS_TEST_LIST_PERF_BUCKET")
	prefix := os.Getenv("AWS_TEST_LIST_PERF_PREFIX")
	if testing.Short() || bucket == "" {
		t.Skip("skipping AWS-specific test")
	}
	if prefix == "" {
		prefix = "."
	}
	key, err := aws.AmbientKey("s3", DeriveForBucket(bucket))
	if err != nil {
		t.Skipf("skipping; couldn't derive key: %s", err)
	}
	b := &BucketFS{
		Key:      key,
		Bucket:   bucket,
		DelayGet: true,
		Ctx:      context.Background(),
	}
	dir, err := b.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	pre, ok := dir.(*Prefix)
	if !ok {
		t.Fatalf("got %T back from BucketFS.Open(%s)", dir, ".")
	}
	start := time.Now()
	walked := 0
	err = fsutil.WalkDir(pre, prefix, "", "", func(p string, d fsutil.DirEntry, err error) error {
		if err != nil {
			return err
		}
		t.Log(p)
		if !d.IsDir() {
			walked++
		}
		if walked >= 100 {
			return fs.SkipDir
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("walked %d files in %s", walked, time.Since(start))
}
