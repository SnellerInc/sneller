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

//go:build amd64
// +build amd64

package vm

//go:noescape
//go:nosplit
func unpivotAtDistinctDeduplicate(rows []vmref, vmbase uintptr, bitvector *uint)

//go:noescape
//go:nosplit
func fillVMrefs(p *[]vmref, v vmref, n int)

//go:noescape
//go:nosplit
func copyVMrefs(p *[]vmref, q *vmref, n int)
