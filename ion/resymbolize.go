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

package ion

// take a buffer (must be valid, sorted symbols, etc.)
// and resymbolize it starting with the empty symbol table,
// and set st to the new (hopefully smaller) symbol table
func resymbolize(dst *Buffer, rng *Ranges, st *Symtab, buf []byte) {
	var newst Symtab
	rs := resymbolizer{
		srctab: st,
		dsttab: &newst,
	}
	rs.resym(dst, buf)

	// resymbolize ranges:
	newm := make(map[symstr]dataRange)
	newp := rng.paths[:0]
	for oldstr, r := range rng.m {
		newstr := oldstr.transcode(&rs)
		newm[newstr] = r
		newp = append(newp, newstr)
	}
	rng.m = newm
	rng.paths = newp
	newst.CloneInto(st)
}
