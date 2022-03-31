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

package main

import (
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/vm"
)

type noTableHandle struct{}

func (h noTableHandle) Open() (vm.Table, error) {
	panic("during planning phase, we will not allow tables to be read.")
}

// Filter implements plan.Filterable.
func (h noTableHandle) Filter(f expr.Node) plan.TableHandle {
	return &filterHandle{filter: f}
}

type filterHandle struct {
	noTableHandle
	filter expr.Node
}
