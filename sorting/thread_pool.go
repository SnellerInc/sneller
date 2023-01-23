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

package sorting

import (
	"sync"
)

type threadPool struct {
	threads  int
	wg       *sync.WaitGroup
	reqMutex sync.Mutex
	requests []sortRequest
	err      error
	closed   bool
	cond     *sync.Cond
}

type sortRequest struct {
	start, end int
	function   SortingFunction
	args       any
}

func NewThreadPool(threads int) ThreadPool {
	pool := &threadPool{
		threads: threads,
		wg:      new(sync.WaitGroup),
	}

	pool.init()
	return pool
}

func (t *threadPool) init() {

	t.cond = sync.NewCond(&t.reqMutex)

	var started sync.WaitGroup

	worker := func(id int) {
		defer t.wg.Done()
		started.Done()

		var request sortRequest
		var n int
		for {
			t.reqMutex.Lock()
			n = len(t.requests)
			for !t.closed && n == 0 {
				t.cond.Wait()
				n = len(t.requests)
			}
			if t.closed {
				t.reqMutex.Unlock()
				break
			}
			if n > 0 {
				request = t.requests[n-1]
				t.requests = t.requests[:n-1]
			}
			t.reqMutex.Unlock()

			if n > 0 {
				request.function(request.start, request.end, request.args, t)
			}
		}
	}

	started.Add(t.threads)
	t.wg.Add(t.threads)
	for i := 0; i < t.threads; i++ {
		go worker(i)
	}

	// Wait for all worker threads to be ready.
	// Otherwise possible condition notification
	// will not reach the workers.
	started.Wait()
}

func (t *threadPool) Enqueue(start, end int, fun SortingFunction, args interface{}) {
	t.reqMutex.Lock()
	if !t.closed {
		t.requests = append(t.requests, sortRequest{start, end, fun, args})
		t.cond.Broadcast()
	}
	t.reqMutex.Unlock()
}

func (t *threadPool) Close(err error) {
	t.reqMutex.Lock()
	t.err = err
	t.closed = true
	t.cond.Broadcast()
	t.reqMutex.Unlock()
}

func (t *threadPool) Wait() error {
	t.wg.Wait()
	return t.err
}
