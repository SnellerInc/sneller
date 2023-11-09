// Copyright (C) 2023 Sneller, Inc.
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

package vm

import (
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/zion/zll"
)

func init() {
	//    zionflatten = zionflattenAVX512BranchlessVarUint
	zionflatten = zionflattenAVX512BranchingVarUint
	//zionflatten = zionflattenAVX512Legacy
}

//lint:ignore U1000 available for use
//go:noescape
func zionflattenAVX512Legacy(shape []byte, buckets *zll.Buckets, fields []vmref, tape []ion.Symbol) (in, out int)

//lint:ignore U1000 available for use
//go:noescape
func zionflattenAVX512BranchingVarUint(shape []byte, buckets *zll.Buckets, fields []vmref, tape []ion.Symbol) (in, out int)

//lint:ignore U1000 available for use
//go:noescape
func zionflattenAVX512BranchlessVarUint(shape []byte, buckets *zll.Buckets, fields []vmref, tape []ion.Symbol) (in, out int)
