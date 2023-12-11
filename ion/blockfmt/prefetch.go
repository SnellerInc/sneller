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

package blockfmt

import (
	"io"
	"sync"
	"sync/atomic"
)

type prefetcher struct {
	wg sync.WaitGroup

	// wanted and current inflight
	// (current is adjusted atomically)
	wantInflight, curInflight int64
}

// doPrefetch wraps the output channel with
// the given number of (maximum) parallel prefetch
// operations and the maximum number of inflight bytes;
// the output channel will be closed after the returned
// channel is closed and all inputs have been processed
func doPrefetch(out chan *Input, parallel int, inflight int64) chan *Input {
	in := make(chan *Input, parallel)
	p := &prefetcher{
		wantInflight: inflight,
	}
	p.wg.Add(parallel)
	for i := 0; i < parallel; i++ {
		go p.work(out, in)
	}
	go func() {
		p.wg.Wait()
		close(out)
	}()
	return in
}

func (p *prefetcher) canPrefetch(n int64) bool {
	for {
		cur := atomic.LoadInt64(&p.curInflight)
		more := cur + n
		if more >= p.wantInflight {
			return false
		}
		if atomic.CompareAndSwapInt64(&p.curInflight, cur, more) {
			return true
		}
	}
}

type wrappedInput struct {
	inner   io.ReadCloser
	size    int64
	parent  *prefetcher
	started bool
}

func (w *wrappedInput) Close() error {
	if w.started {
		atomic.AddInt64(&w.parent.curInflight, -w.size)
		w.started = false
	}
	return w.inner.Close()
}

func (w *wrappedInput) Read(p []byte) (int, error) {
	if !w.started {
		atomic.AddInt64(&w.parent.curInflight, w.size)
		w.started = true
	}
	return w.inner.Read(p)
}

func (p *prefetcher) work(outputs, inputs chan *Input) {
	defer p.wg.Done()
loop:
	for in := range inputs {
		if !in.canPrefetch() {
			outputs <- in
			continue
		}
		w := &wrappedInput{
			inner:  in.R,
			size:   in.Size,
			parent: p,
		}
		in.R = w
		// input can be consumed immediately:
		select {
		case outputs <- in:
			continue loop
		default:
		}
		if p.canPrefetch(in.Size) {
			w.started = true
			w.inner.Read([]byte{})
		}
		outputs <- in
	}
}
