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

package vm

import (
	"math/rand"
	"slices"
	"testing"
)

func TestSpan(t *testing.T) {
	check := func(t *testing.T, got, want int) {
		if got != want {
			t.Helper()
			t.Fatalf("%d != %d", got, want)
		}
	}

	validate := func(t *testing.T, s *spanalloc) {
		spanorder := func(a, b span) int {
			return a.pos - b.pos
		}
		if !slices.IsSortedFunc(s.used, spanorder) {
			t.Helper()
			t.Fatalf("used not sorted: %#v", s.used)
		}
		if !slices.IsSortedFunc(s.free, spanorder) {
			t.Helper()
			t.Fatalf("free not sorted: %#v", s.free)
		}

		// check for gapless accounting of all
		// the possible spans up to lastused
		pos := 0
		end := s.lastused()
		i, j := 0, 0
		for pos < end {
			var cur *span
			if j >= len(s.free) || s.used[i].pos < s.free[j].pos {
				cur = &s.used[i]
				i++
			} else {
				cur = &s.free[j]
				j++
			}
			if cur.pos != pos {
				t.Helper()
				t.Logf("used: %v", s.used[i-1:])
				t.Logf("free: %v", s.free[j-1:])
				t.Fatalf("unexpected pos %d; expected %d", cur.pos, pos)
			}
			pos = cur.end()
		}

		if len(s.free) > 0 && s.free[len(s.free)-1].pos > s.lastused() {
			t.Helper()
			t.Fatalf("last free element (%v) beyond last used position %d", s.free[len(s.free)-1], s.lastused())
		}
	}

	t.Run("simple", func(t *testing.T) {
		s := spanalloc{}
		defer func() {
			if t.Failed() {
				t.Logf("used: %#v", s.used)
				t.Logf("free: %#v", s.free)
			}
		}()

		p0 := s.get(2, 1)
		validate(t, &s)
		check(t, p0, 0)
		check(t, s.lastused(), 2)
		check(t, len(s.used), 1)

		p1 := s.get(128, 64)
		validate(t, &s)
		check(t, p1, 64)
		check(t, len(s.used), 2)
		check(t, s.lastused(), 128+64)
		check(t, s.max, 128+64)

		s.drop(p0) // should trigger a merge
		validate(t, &s)
		check(t, len(s.used), 1)
		check(t, len(s.free), 1)

		p2 := s.get(32, 32)
		validate(t, &s)
		check(t, p2, 0)
		check(t, len(s.used), 2) // p1 and p2
		check(t, len(s.free), 1) // from 32 to 64

		s.drop(p1)
		validate(t, &s)
		check(t, s.lastused(), 32)
		check(t, len(s.used), 1)
		check(t, len(s.free), 0) // all ranges unspecified or used

		s.drop(p2)
		validate(t, &s)
		check(t, len(s.free), 0) // all free ranges coalesced and deleted
		check(t, s.lastused(), 0)
	})

	t.Run("randomized", func(t *testing.T) {
		// simulate typical allocation sizes:
		classes := []regclass{regK, regV, regS, regL, regH, regB}
		for i := 0; i < 5000; i++ {
			var s spanalloc
			selected := make([]regclass, 50)
			results := make([]int, 50)

			// pick 50 elements to allocate, with
			// sizes selected pseudorandomly from
			// the list of sizes + alignments above
			for i := range selected {
				selected[i] = classes[rand.Intn(len(classes))]
			}
			// ... allocate them:
			for i := range results {
				results[i] = s.get(selected[i].size(), selected[i].align())
				validate(t, &s)
			}
			check(t, s.lastused(), s.max)

			// free half the results in a random order
			rand.Shuffle(len(results), func(i, j int) {
				results[i], results[j] = results[j], results[i]
				selected[i], selected[j] = selected[j], selected[i]
			})
			for i := range results[:len(results)/2] {
				s.drop(results[i])
				validate(t, &s)
			}
			// allocate them again in the new order:
			for i := range results[:len(results)/2] {
				results[i] = s.get(selected[i].size(), selected[i].align())
				validate(t, &s)
			}

			// ... now de-allocate everything
			for i := range results {
				s.drop(results[i])
				validate(t, &s)
			}

			// should be fully reset:
			check(t, s.lastused(), 0)
			check(t, len(s.free), 0)
		}
	})
}
