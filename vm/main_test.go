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
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	var leakbuf bytes.Buffer
	ret := 0
	LeakCheck(&leakbuf, func() {
		ret = m.Run()
	})
	if ret == 0 && leakbuf.Len() > 0 {
		ret = 2
		fmt.Println("memory leaks:")
		os.Stdout.Write(leakbuf.Bytes())
	}
	os.Exit(ret)
}
