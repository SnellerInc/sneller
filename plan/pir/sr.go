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

package pir

import (
	"github.com/SnellerInc/sneller/expr"
)

// strengthReduce applies strength reduction rules until fixed point is achieved
func strengthReduce(b *Trace) {
	context := &fpoContext{trace: b}
	srFPO.optimize(b, context)
}

// Fixed-Point Optimizer for Strength Reduction purposes
var srFPO fixedPointOptimizer

func init() {
	srFPO = newFixedPointOptimizer(
		srDistinctUnpivot,
		uniqueReplacement,
	)
}

// srDistinctUnpivot rule is applicable to DISTINCT-UNPIVOT subtraces
func srDistinctUnpivot(d *Distinct, _ *fpoContext) (Step, fpoStatus) {
	if u, ok := d.par.(*Unpivot); ok {
		if u.Ast.As == nil && u.Ast.At != nil && len(d.Columns) == 1 && expr.IsIdentifier(d.Columns[0], *u.Ast.At) {
			// Unpivot AT x
			// Distinct x
			// =>
			// UnpivotAtDistinct x
			return &UnpivotAtDistinct{parented: u.parented, Ast: u.Ast}, fpoReplace
		}
	}
	return nil, fpoIntact
}
