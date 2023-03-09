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

package plan

import (
	"github.com/SnellerInc/sneller/vm"
)

func (t *Tree) exec(dst vm.QuerySink, ep *ExecParams) error {
	ep.get = func(i int) TableHandle {
		return t.Inputs[i].Handle
	}
	return t.Root.exec(dst, ep)
}

func (n *Node) exec(dst vm.QuerySink, ep *ExecParams) error {
	i := n.Input
	var h TableHandle
	if i >= 0 {
		h = ep.get(i)
	}
	return n.Op.exec(dst, h, ep)
}
