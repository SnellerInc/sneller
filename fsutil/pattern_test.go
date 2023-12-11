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

package fsutil

import (
	"testing"
)

func TestMatch(t *testing.T) {
	t.Run("pattern", testPattern)
	t.Run("expand", testExpand)
	t.Run("toglob", testToGlob)
}

func testPattern(t *testing.T) {
	var mr Matcher
	n := 0
	run := func(pattern, name string, want bool, wanterr error) {
		t.Helper()
		n++
		found, err := mr.Match(pattern, name)
		if found != want {
			t.Errorf("case %d: expected found = %v, got %v", n, want, found)
		}
		if err != wanterr {
			t.Errorf("case %d: expected err = %v, got %v", n, wanterr, err)
		}
	}
	// test cases from stdlib
	run("abc", "abc", true, nil)
	run("*", "abc", true, nil)
	run("*c", "abc", true, nil)
	run("a*", "a", true, nil)
	run("a*", "abc", true, nil)
	run("a*", "ab/c", false, nil)
	run("a*/b", "abc/b", true, nil)
	run("a*/b", "a/c/b", false, nil)
	run("a*b*c*d*e*/f", "axbxcxdxe/f", true, nil)
	run("a*b*c*d*e*/f", "axbxcxdxexxx/f", true, nil)
	run("a*b*c*d*e*/f", "axbxcxdxe/xxx/f", false, nil)
	run("a*b*c*d*e*/f", "axbxcxdxexxx/fff", false, nil)
	run("a*b?c*x", "abxbbxdbxebxczzx", true, nil)
	run("a*b?c*x", "abxbbxdbxebxczzy", false, nil)
	run("ab[c]", "abc", true, nil)
	run("ab[b-d]", "abc", true, nil)
	run("ab[e-g]", "abc", false, nil)
	run("ab[^c]", "abc", false, nil)
	run("ab[^b-d]", "abc", false, nil)
	run("ab[^e-g]", "abc", true, nil)
	run("a\\*b", "a*b", true, nil)
	run("a\\*b", "ab", false, nil)
	run("a?b", "a☺b", true, nil)
	run("a[^a]b", "a☺b", true, nil)
	run("a???b", "a☺b", false, nil)
	run("a[^a][^a][^a]b", "a☺b", false, nil)
	run("[a-ζ]*", "α", true, nil)
	run("*[a-ζ]", "A", false, nil)
	run("a?b", "a/b", false, nil)
	run("a*b", "a/b", false, nil)
	run("[\\]a]", "]", true, nil)
	run("[\\-]", "-", true, nil)
	run("[x\\-]", "x", true, nil)
	run("[x\\-]", "-", true, nil)
	run("[x\\-]", "z", false, nil)
	run("[\\-x]", "x", true, nil)
	run("[\\-x]", "-", true, nil)
	run("[\\-x]", "a", false, nil)
	run("[]a]", "]", false, ErrBadPattern)
	run("[-]", "-", false, ErrBadPattern)
	run("[x-]", "x", false, ErrBadPattern)
	run("[x-]", "-", false, ErrBadPattern)
	run("[x-]", "z", false, ErrBadPattern)
	run("[-x]", "x", false, ErrBadPattern)
	run("[-x]", "-", false, ErrBadPattern)
	run("[-x]", "a", false, ErrBadPattern)
	run("\\", "a", false, ErrBadPattern)
	run("[a-b-c]", "a", false, ErrBadPattern)
	run("[", "a", false, ErrBadPattern)
	run("[^", "a", false, ErrBadPattern)
	run("[^bc", "a", false, ErrBadPattern)
	run("a[", "a", false, ErrBadPattern)
	run("a[", "ab", false, ErrBadPattern)
	run("a[", "x", false, ErrBadPattern)
	run("a/b[", "x", false, ErrBadPattern)
	run("*x", "xxx", true, nil)
	// empty names
	run(``, "", true, nil)
	run(`*`, "", true, nil)
	run(`foo`, "", false, nil)
	run(`f*`, "", false, nil)
	run(`*o`, "", false, nil)
	run(`{bar}`, "", false, nil)
	run(`f{bar}`, "", false, nil)
	run(`{bar}o`, "", false, nil)
	// simple cases
	run(`{bar}`, "foo", true, nil)
	run(`f{bar}`, "foo", true, nil)
	run(`{bar}o`, "foo", true, nil)
	run(`f{bar}o`, "foo", true, nil)
	run(`{bar}foo`, "foo", false, nil)
	run(`foo{bar}`, "foo", false, nil)
	run(`fo{bar}o`, "foo", false, nil)
	run(`fo{_}o`, "foo", false, nil)
	// multiple segments
	run(`foo/{x}`, "foo/bar", true, nil)
	run(`{x}/bar`, "foo/bar", true, nil)
	run(`*/*`, "foo/bar", true, nil)
	run(`f{x}/bar`, "foo/bar", true, nil)
	run(`f{x}/b{y}`, "foo/bar", true, nil)
	run(`f{x}o/b{y}r`, "foo/bar", true, nil)
	run(`{x}o/{y}r`, "foo/bar", true, nil)
	run(`foo{x}/bar{y}`, "foo/bar", false, nil)
	run(`foo{x}/bar{y}`, "foo/bar", false, nil)
	run(`{x}foo/{y}bar`, "foo/bar", false, nil)
	run(`fo{x}o/b{y}ar`, "foo/bar", false, nil)
	run(`{x}/{y}/{z}`, "foo/bar/baz", true, nil)
	run(`{x}/{y}/{z}/`, "foo/bar/baz/", true, nil)
	run(`{x}/{y}/{z}`, "foo/bar/baz/", false, nil)
	run(`{x}/{y}/{z}/`, "foo/bar/baz", false, nil)
	// escaped characters in pattern
	run(`\{foo}`, "foo", false, nil)
	run(`\{foo}`, "{foo}", true, nil)
	run(`{foo\}`, "{foo}", false, ErrBadPattern)
	run(`{foo\}}`, "{foo}", false, ErrBadPattern)
	// adjacent wildcards
	run(`**`, "foo", true, nil)
	run(`*{bar}`, "foo", false, ErrBadPattern)
	run(`{bar}*`, "foo", false, ErrBadPattern)
	run(`{foo}{bar}`, "foo", false, ErrBadPattern)
	// bad cases
	run(`{}`, "", false, ErrBadPattern)
	run(`{`, "", false, ErrBadPattern)
	run(`{foo`, "", false, ErrBadPattern)
	run(`{bar}/{bar}`, "foo/bar", false, ErrBadPattern)
	run(`{b@r}`, "bar", false, ErrBadPattern)
	run(`foo/{b@r}`, "bar/bar", false, ErrBadPattern)
}

func testExpand(t *testing.T) {
	var mr Matcher
	n := 0
	run := func(pattern, name, template, result string, merr, eerr error) {
		t.Helper()
		n++
		found, err := mr.Match(pattern, name)
		if !found {
			t.Errorf("case %d: expected match", n)
			return
		}
		if err != merr {
			t.Errorf("case %d: expected match err = %v, got %v", n, merr, err)
		}
		if merr != nil {
			// ignore results
			return
		}
		got, err := mr.Expand(template)
		if err != eerr {
			t.Errorf("case %d: expected expand err = %v, got %v", n, eerr, err)
		}
		if string(got) != result {
			t.Errorf("case %d: expected result = %q, got %q", n, result, got)
		}
	}
	run("{x}", "bar", "$x", "bar", nil, nil)
	run("{_}", "bar", "$_", "bar", nil, nil)
	run("{x}", "bar", "$y", "", nil, ErrBadPattern)
	run("{x}", "bar", "${}", "", nil, ErrBadPattern)
	run("{x}", "bar", "$x$y", "", nil, ErrBadPattern)
	run("{x}", "bar", "$$x", "$x", nil, nil)
	run("{x}", "bar", "$$$x", "$bar", nil, nil)
	run("{x}", "bar", "$", "", nil, ErrBadPattern)
	run("{x}", "bar", "$$", "$", nil, nil)
	run("{x}", "bar", "foo-$x-baz", "foo-bar-baz", nil, nil)
	run("{x}", "bar", "$x-$x", "bar-bar", nil, nil)
	run("{x}", "bar", "${x}", "bar", nil, nil)
	run("{_}", "bar", "${_}", "bar", nil, nil)
	run("{x_x}", "bar", "${x_x}", "bar", nil, nil)
	run("{x}", "bar", "foo${x}baz", "foobarbaz", nil, nil)
	run("{x}", "bar", "${x}${x}", "barbar", nil, nil)
	run("{x}-{y}", "foo-bar", "$x$y", "foobar", nil, nil)
	run("{x}-{y}", "foo-bar", "$y$x", "barfoo", nil, nil)
	run("{x}/{y}", "foo/bar", "$y$x", "barfoo", nil, nil)
	run("f{x}o-b{y}r", "foo-bar", "$x$y", "oa", nil, nil)
	run("f{x}o/b{y}r", "foo/bar", "$x$y", "oa", nil, nil)
	run("fo{x}/{y}ar", "foo/bar", "$x$y", "ob", nil, nil)
	run(
		"s3://bucket/f[aeiou]?/*/baz-{yyyy}-{mm}-{dd}.tar.gz",
		"s3://bucket/foo/bar/baz-2022-10-20.tar.gz",
		"table-$yyyy-$mm-$dd",
		"table-2022-10-20",
		nil, nil,
	)
}

func testToGlob(t *testing.T) {
	n := 0
	run := func(pattern, want string, wanterr error) {
		t.Helper()
		n++
		got, err := ToGlob(pattern)
		if err != wanterr {
			t.Errorf("case %d: expected err = %v, got %v", n, wanterr, err)
		}
		if wanterr != nil {
			// ignore results
			return
		}
		if got != want {
			t.Errorf("case %d: expected %q, got %q", n, want, got)
		}
	}
	run("{x}", "*", nil)
	run("{x}-{y}", "*-*", nil)
	run("{x}-bar", "*-bar", nil)
	run("foo-{y}", "foo-*", nil)
	run("foo-bar", "foo-bar", nil)
	run("*-bar", "*-bar", nil)
	run("foo-*", "foo-*", nil)
	run("foo/{x}", "foo/*", nil)
	run("foo/{x}-{y}", "foo/*-*", nil)
	run("foo/{x}-bar", "foo/*-bar", nil)
	run("foo/foo-{y}", "foo/foo-*", nil)
	run("foo/foo-bar", "foo/foo-bar", nil)
	run("foo/*-bar", "foo/*-bar", nil)
	run("foo/foo-*", "foo/foo-*", nil)
}

func BenchmarkMatchExpand(b *testing.B) {
	name := "s3://bucket/foo/bar/baz-2022-10-06.tar.gz"
	pat := "s3://bucket/foo/bar/baz-{yyyy}-{mm}-{dd}.tar.gz"
	tmpl := "table-$yyyy-$mm-$dd"
	b.ReportAllocs() // should be zero
	var mr Matcher
	for i := 0; i < b.N; i++ {
		mr.Match(pat, name)
		mr.Expand(tmpl)
	}
}
