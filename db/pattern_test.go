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
	"testing"
)

func TestMatch(t *testing.T) {
	t.Run("pattern", testPattern)
	t.Run("expand", testExpand)
	t.Run("detemplate", testDetemplate)
	t.Run("hascapture", testHasCapture)
	t.Run("toglob", testToGlob)
}

func testPattern(t *testing.T) {
	var mr matcher
	n := 0
	run := func(pattern, name string, want bool, wanterr error) {
		t.Helper()
		err := mr.match(pattern, name, "")
		if mr.found != want {
			t.Errorf("case %d: expected found = %v, got %v", n, want, mr.found)
		}
		if err != wanterr {
			t.Errorf("case %d: expected err = %v, got %v", n, wanterr, err)
		}
		n++
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
	var mr matcher
	n := 0
	run := func(pattern, name, template, glob, result string, wanterr error) {
		t.Helper()
		err := mr.match(pattern, name, template)
		if !mr.found {
			t.Errorf("case %d: expected match", n)
			return
		}
		if err != wanterr {
			t.Errorf("case %d: expected err = %v, got %v", n, wanterr, err)
		}
		if wanterr != nil {
			// ignore results
			return
		}
		if string(mr.glob) != glob {
			t.Errorf("case %d: expected glob = %q, got %q", n, glob, mr.glob)
		}
		if string(mr.result) != result {
			t.Errorf("case %d: expected result = %q, got %q", n, result, mr.result)
		}
		n++
	}
	// template expansion
	run("{x}", "bar", "$x", "bar", "bar", nil)
	run("{_}", "bar", "$_", "bar", "bar", nil)
	run("{x}", "bar", "$y", "", "", ErrBadPattern)
	run("{x}", "bar", "${}", "", "", ErrBadPattern)
	run("{x}", "bar", "$x$y", "", "", ErrBadPattern)
	run("{x}", "bar", "$$x", "bar", "$x", nil)
	run("{x}", "bar", "$$$x", "bar", "$bar", nil)
	run("{x}", "bar", "$", "", "", ErrBadPattern)
	run("{x}", "bar", "$$", "bar", "$", nil)
	run("{x}", "bar", "foo-$x-baz", "bar", "foo-bar-baz", nil)
	run("{x}", "bar", "$x-$x", "bar", "bar-bar", nil)
	run("{x}", "bar", "${x}", "bar", "bar", nil)
	run("{_}", "bar", "${_}", "bar", "bar", nil)
	run("{x_x}", "bar", "${x_x}", "bar", "bar", nil)
	run("{x}", "bar", "foo${x}baz", "bar", "foobarbaz", nil)
	run("{x}", "bar", "${x}${x}", "bar", "barbar", nil)
	run("{x}-{y}", "foo-bar", "$x$y", "foo-bar", "foobar", nil)
	run("{x}-{y}", "foo-bar", "$y$x", "foo-bar", "barfoo", nil)
	run("{x}/{y}", "foo/bar", "$y$x", "foo/bar", "barfoo", nil)
	run("f{x}o-b{y}r", "foo-bar", "$x$y", "foo-bar", "oa", nil)
	run("f{x}o/b{y}r", "foo/bar", "$x$y", "foo/bar", "oa", nil)
	run("fo{x}/{y}ar", "foo/bar", "$x$y", "foo/bar", "ob", nil)
	// glob expansion
	run("{x}-*-{y}", "a-b-c-d", "$x-$y", "a-*-c-d", "a-c-d", nil)
	run("*-{x}/*", "foo-bar/baz", "", "*-bar/*", "", nil)
	run("[abc]-{x}-?oo", "b-bar-foo", "", "[abc]-bar-?oo", "", nil)
	run(`{x}-\{bar}-\*`, "foo-{bar}-*", "", `foo-\{bar}-\*`, "", nil)
	// something vaguely realistic
	run(
		"s3://bucket/f[aeiou]?/*/baz-{yyyy}-{mm}-{dd}.tar.gz",
		"s3://bucket/foo/bar/baz-2022-10-20.tar.gz",
		"table-$yyyy-$mm-$dd",
		"s3://bucket/f[aeiou]?/*/baz-2022-10-20.tar.gz",
		"table-2022-10-20",
		nil,
	)
}

func testDetemplate(t *testing.T) {
	n := 0
	run := func(template, want string, wantok bool, wanterr error) {
		t.Helper()
		name, ok, err := detemplate(template)
		if ok != wantok {
			t.Errorf("case %d: expected ok = %v, got %v", n, wantok, ok)
		}
		if name != want {
			t.Errorf("case %d: expected name = %q, got %q", n, want, name)
		}
		if err != wanterr {
			t.Errorf("case %d: expected err = %v, got %v", n, wanterr, err)
		}
		n++
	}
	run("foo", "foo", true, nil)
	run("$$foo", "$foo", true, nil)
	run("foo$$", "foo$", true, nil)
	run("$foo", "", false, nil)
	run("foo-$bar", "", false, nil)
	run("foo-$bar", "", false, nil)
	run("foo-$$bar", "foo-$bar", true, nil)
	run("foo$", "", false, ErrBadPattern)
	run("foo${bar", "", false, ErrBadPattern)
}

func testHasCapture(t *testing.T) {
	n := 0
	run := func(pattern string, want, wantok bool) {
		t.Helper()
		got, ok := hascapture(pattern)
		if got != want {
			t.Errorf("case %d: expected %v, got %v", n, want, got)
		}
		if ok != wantok {
			t.Errorf("case %d: expected ok = %v, got %v", n, wantok, ok)
		}
		n++
	}
	run("", false, true)
	run("foo", false, true)
	run(`foo-\{bar}`, false, true)
	run("{x}", true, true)
	run("{x}.json", true, true)
	run("foo-{x}", true, true)
	run("foo-{x}.json", true, true)
}

func testToGlob(t *testing.T) {
	n := 0
	run := func(pattern, want string, wanterr error) {
		t.Helper()
		got, err := toglob(pattern)
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
		n++
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

func BenchmarkMatch(b *testing.B) {
	name := "s3://bucket/foo/bar/baz-2022-10-06.tar.gz"
	pat := "s3://bucket/foo/bar/baz-{yyyy}-{mm}-{dd}.tar.gz"
	tmpl := "table-$yyyy-$mm-$dd"
	b.ReportAllocs() // should be zero
	var mr matcher
	for i := 0; i < b.N; i++ {
		mr.match(pat, name, tmpl)
	}
}
