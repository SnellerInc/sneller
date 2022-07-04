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

package partiql

import (
	"sort"

	"github.com/SnellerInc/sneller/expr"
)

// kwterms is the list of keyword terms
// in sorted order so that keywords can be
// efficiently identified without expensive
// case normalization or allocation
var kwterms termlist

var aggterms termlist

func init() {
	type pair struct {
		name string
		term int
	}
	for _, pair := range []pair{
		{"SELECT", SELECT},
		{"AND", AND},
		{"AS", AS},
		{"ASC", ASC},
		{"CAST", CAST},
		{"CONCAT", CONCAT},
		{"COALESCE", COALESCE},
		{"DATE_ADD", DATE_ADD},
		{"DATE_DIFF", DATE_DIFF},
		{"DESC", DESC},
		{"DISTINCT", DISTINCT},
		{"DATE_TRUNC", DATE_TRUNC},
		{"EXTRACT", EXTRACT},
		{"EXISTS", EXISTS},
		{"UNION", UNION},
		{"OR", OR},
		{"ON", ON},
		{"OVER", OVER},
		{"FROM", FROM},
		{"WHERE", WHERE},
		{"GROUP", GROUP},
		{"ORDER", ORDER},
		{"BY", BY},
		{"HAVING", HAVING},
		{"LIMIT", LIMIT},
		{"OFFSET", OFFSET},
		{"ILIKE", ILIKE},
		{"LIKE", LIKE},
		{"NULL", NULL},
		{"NULLS", NULLS},
		{"NULLIF", NULLIF},
		{"PARTITION", PARTITION},
		{"MISSING", MISSING},
		{"IS", IS},
		{"IN", IN},
		{"INTO", INTO},
		{"NOT", NOT},
		{"ALL", ALL},
		{"LEFT", LEFT},
		{"RIGHT", RIGHT},
		{"CROSS", CROSS},
		{"JOIN", JOIN},
		{"INNER", INNER},
		{"TRUE", TRUE},
		{"FALSE", FALSE},
		{"BETWEEN", BETWEEN},
		{"CASE", CASE},
		{"WHEN", WHEN},
		{"THEN", THEN},
		{"ELSE", ELSE},
		{"END", END},
		{"VALUE", VALUE},
		{"FIRST", FIRST},
		{"LAST", LAST},
		{"UTCNOW", UTCNOW},
		{"WITH", WITH},
		{"FILTER", FILTER},
	} {
		code, ok := wordcode([]byte(pair.name))
		if !ok {
			panic(pair.name + " not all ascii characters?")
		}
		kwterms = append(kwterms, node{selfcode: code, terminal: pair.term})
	}
	sort.Sort(kwterms)

	for _, pair := range []pair{
		{"COUNT", int(expr.OpCount)},
		{"SUM", int(expr.OpSum)},
		{"MIN", int(expr.OpMin)},
		{"MAX", int(expr.OpMax)},
		{"EARLIEST", int(expr.OpEarliest)},
		{"LATEST", int(expr.OpLatest)},
		{"BOOL_AND", int(expr.OpBoolAnd)},
		{"BOOL_OR", int(expr.OpBoolOr)},
		{"AVG", int(expr.OpAvg)},
		{"BIT_AND", int(expr.OpBitAnd)},
		{"BIT_OR", int(expr.OpBitOr)},
		{"BIT_XOR", int(expr.OpBitXor)},
	} {
		code, ok := wordcode([]byte(pair.name))
		if !ok {
			panic(pair.name + " not all ascii characters?")
		}
		aggterms = append(aggterms, node{selfcode: code, terminal: pair.term})
	}
	sort.Sort(aggterms)
	expr.IsKeyword = func(x string) bool {
		return kwterms.contains(x) || aggterms.contains(x)
	}
}

type node struct {
	selfcode uint64 // integer formed from term characters,
	terminal int
}

// termlist is a sorted list of term nodes
type termlist []node

func (t termlist) Len() int {
	return len(t)
}

func (t termlist) Less(i, j int) bool {
	return t[i].selfcode < t[j].selfcode
}

func (t termlist) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func charcode(b byte) (uint64, bool) {
	if b >= 'a' && b <= 'z' {
		return uint64(b-'a') + 1, true
	}
	if b >= 'A' && b <= 'Z' {
		return uint64(b-'A') + 1, true
	}
	if b == '_' {
		return uint64(26), true
	}
	if b >= '0' && b <= '4' {
		return uint64(b-'0') + 27, true
	}
	return 0, false
}

// wordcode produces an integer from
// a string of ascii characters
//
// the wordcode is case-insensitive
func wordcode(buf []byte) (uint64, bool) {
	code := uint64(0)

	// Each character requires 5 bits, 64/5 = 12.8.
	if len(buf) > 12 {
		return 0, false
	}
	for i := range buf {
		bits, ok := charcode(buf[i])
		if !ok {
			return 0, false
		}
		code = (code << 5) | bits
	}
	return code, true
}

// same as wordcode, but for strings
func wordcodestr(s string) (uint64, bool) {
	if len(s) > 12 {
		return 0, false
	}
	code := uint64(0)
	for i := range s {
		bits, ok := charcode(s[i])
		if !ok {
			return 0, false
		}
		code = (code << 5) | bits
	}
	return code, true
}

func (t termlist) get(b []byte) int {
	code, ok := wordcode(b)
	if !ok {
		return -1
	}
	idx := sort.Search(len(t), func(i int) bool {
		return t[i].selfcode >= code
	})
	if idx >= len(t) || t[idx].selfcode != code {
		return -1
	}
	return t[idx].terminal
}

func (t termlist) contains(s string) bool {
	code, ok := wordcodestr(s)
	if !ok {
		return false
	}
	idx := sort.Search(len(t), func(i int) bool {
		return t[i].selfcode >= code
	})
	return idx < len(t) && t[idx].selfcode == code
}
