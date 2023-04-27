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
