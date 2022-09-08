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

package cgroup

import (
	"os"
	"strings"
	"testing"
)

func TestCgroup(t *testing.T) {
	root, err := Root()
	if err != nil {
		t.Skip("couldn't find cgroup root")
	}
	self, err := Self()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(self), string(root)) {
		t.Errorf("current cgroup %s not within root %s", self, root)
	}
	t.Log("in cgroup", self)
	owned, err := self.IsDelegated(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("in delegated cgroup: %v", owned)
	if !owned {
		return
	}
	sub, err := self.Create("test", true)
	if err != nil {
		t.Fatal(err)
	}
	err = sub.Remove()
	if err != nil {
		t.Fatal("removing sub:", err)
	}
}
