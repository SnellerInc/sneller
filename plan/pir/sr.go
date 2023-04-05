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
