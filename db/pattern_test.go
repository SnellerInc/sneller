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
}

func testPattern(t *testing.T) {
	n := 0
	run := func(pattern, name string, want bool, wanterr error) {
		t.Helper()
		matched, _, err := match(pattern, name, "")
		if matched != want {
			t.Errorf("case %d: expected matched = %v, got %v", n, want, matched)
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
	n := 0
	run := func(pattern, name, template, want string, wanterr error) {
		t.Helper()
		matched, expanded, err := match(pattern, name, template)
		if !matched {
			t.Errorf("case %d: expected match", n)
			return
		}
		if expanded != want {
			t.Errorf("case %d: expected expanded = %q, got %q", n, want, expanded)
		}
		if err != wanterr {
			t.Errorf("case %d: expected err = %v, got %v", n, wanterr, err)
		}
		n++
	}
	run("{x}", "bar", "$x", "bar", nil)
	run("{_}", "bar", "$_", "bar", nil)
	run("{x}", "bar", "$y", "", ErrBadPattern)
	run("{x}", "bar", "${}", "", ErrBadPattern)
	run("{x}", "bar", "$x$y", "", ErrBadPattern)
	run("{x}", "bar", "$$x", "$x", nil)
	run("{x}", "bar", "$$$x", "$bar", nil)
	run("{x}", "bar", "$", "", ErrBadPattern)
	run("{x}", "bar", "$$", "$", nil)
	run("{x}", "bar", "foo-$x-baz", "foo-bar-baz", nil)
	run("{x}", "bar", "$x-$x", "bar-bar", nil)
	run("{x}", "bar", "${x}", "bar", nil)
	run("{_}", "bar", "${_}", "bar", nil)
	run("{x_x}", "bar", "${x_x}", "bar", nil)
	run("{x}", "bar", "foo${x}baz", "foobarbaz", nil)
	run("{x}", "bar", "${x}${x}", "barbar", nil)
	run("{x}-{y}", "foo-bar", "$x$y", "foobar", nil)
	run("{x}-{y}", "foo-bar", "$y$x", "barfoo", nil)
	run("{x}/{y}", "foo/bar", "$y$x", "barfoo", nil)
	run("f{x}o-b{y}r", "foo-bar", "$x$y", "oa", nil)
	run("f{x}o/b{y}r", "foo/bar", "$x$y", "oa", nil)
	run("fo{x}/{y}ar", "foo/bar", "$x$y", "ob", nil)
	run("{x}-*-{y}", "a-b-c-d", "$x-$y", "a-c-d", nil)
}

func BenchmarkMatch(b *testing.B) {
	name := "s3://bucket/foo/bar/baz-2022-10-06.tar.gz"
	pat := "s3://bucket/foo/bar/baz-{yyyy}-{mm}-{dd}.tar.gz"
	tmpl := "table-$yyyy-$mm-$dd"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		match(pat, name, tmpl)
	}
}
