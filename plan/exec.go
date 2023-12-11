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

package plan

import (
	"github.com/SnellerInc/sneller/vm"
)

func (t *Tree) exec(dst vm.QuerySink, ep *ExecParams) error {
	ep.get = func(i int) *Input {
		if t.Inputs[i] == nil {
			panic("nil input?")
		}
		return t.Inputs[i]
	}
	return t.Root.exec(dst, ep)
}

func (n *Node) exec(dst vm.QuerySink, ep *ExecParams) error {
	i := n.Input
	var src *Input
	if i >= 0 {
		src = ep.get(i)
	}
	return n.Op.exec(dst, src, ep)
}
