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
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/zion/zll"
)

func zionFlattenAsm(shape []byte, buckets *zll.Buckets, fields []vmref, tape []ion.Symbol) (int, int) {
	return zionflattenAVX512BranchlessVarUint(shape, buckets, fields, tape)
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
