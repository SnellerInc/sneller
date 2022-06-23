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
	requests chan sortRequest
	errMutex sync.Mutex
	err      error
}

type sortRequest struct {
	start, end int
	function   SortingFunction
	args       interface{}
}

func NewThreadPool(threads int) ThreadPool {
	pool := threadPool{
		threads:  threads,
		wg:       new(sync.WaitGroup),
		requests: make(chan sortRequest)}

	pool.init()
	return &pool
}

func (t *threadPool) init() {

	worker := func(id int) {
		defer t.wg.Done()

		for request := range t.requests {
			request.function(request.start, request.end, request.args, t)
		}
	}

	for i := 0; i < t.threads; i++ {
		t.wg.Add(1)
		go worker(i)
	}
}

func (t *threadPool) Enqueue(start, end int, fun SortingFunction, args interface{}) {
	go func() {
		t.requests <- sortRequest{start, end, fun, args}
	}()
}

func (t *threadPool) Close(err error) {
	t.errMutex.Lock()
	defer t.errMutex.Unlock()
	t.err = err
	close(t.requests)
}

func (t *threadPool) Wait() error {
	t.wg.Wait()
	return t.err
}
