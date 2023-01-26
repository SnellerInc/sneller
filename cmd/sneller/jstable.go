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
	"fmt"
	"io"

	"github.com/SnellerInc/sneller/jsonrl"
	"github.com/SnellerInc/sneller/vm"
)

// jstable is a vm.Table implementation
// that does real-time transcoding of NDJSON
// in parallel
type jstable struct {
	in   io.ReaderAt
	size int64
}

type jsquery struct {
	dst vm.QuerySink
}

func (j *jsquery) Close() error {
	// A little hack: in the case of query execution, that's
	// the responsibility of executor (exec.go) to close the
	// output sink. But also jsonl.Splitter closes the sink.
	// Thus jsquery, used only by Splitter, prevents from
	// double closing the top-level query sink.
	return nil
}

func (j *jsquery) Open() (io.WriteCloser, error) {
	return j.dst.Open()
}

func (j *jsquery) CloseError(err error) {
	exit(fmt.Errorf("jsquery: %s", err))
}

func (j *jstable) WriteChunks(dst vm.QuerySink, parallel int) error {
	if parallel <= 0 {
		parallel = 1
	}
	sp := jsonrl.Splitter{
		Alignment:   1024 * 1024,
		MaxParallel: parallel,
		Output:      &jsquery{dst: dst},
	}
	const maxWindowSize = 8 * 1024 * 1024
	const smallInput = 64 * 1024
	if j.size < smallInput {
		// do not bother spawning more threads if we have small input
		sp.WindowSize = smallInput
		sp.MaxParallel = 1
	} else if j.size/int64(parallel) < maxWindowSize {
		sp.WindowSize = int(j.size / int64(parallel))
	} else {
		sp.WindowSize = maxWindowSize
	}

	return sp.Split(j.in, j.size)
}
