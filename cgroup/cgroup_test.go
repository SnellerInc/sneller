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

//go:build linux

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
