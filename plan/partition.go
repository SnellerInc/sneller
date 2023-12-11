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
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync"

	"github.com/SnellerInc/sneller/ion"
)

type frame uint32

type framekind uint32

const (
	framesize = 4
	maxframe  = (1 << 24) - 1
)

const (
	// zero frame is invalid
	_ framekind = iota

	// client-to-server frames
	framestart

	// server-to-client frames
	framedata // output query data
	frameerr  // query encountered an error
	framefin  // no more query data
)

func (f frame) kind() framekind {
	return framekind(f >> 24)
}

func (f frame) length() int {
	return int(f & 0xffffff)
}

func (f frame) put(dst []byte) {
	binary.LittleEndian.PutUint32(dst, uint32(f))
}

func getframe(src []byte) frame {
	return frame(binary.LittleEndian.Uint32(src))
}

type server struct {
	run    Runner
	initfs func(ion.Datum) (fs.FS, error)

	pipe io.ReadWriteCloser
	rd   *bufio.Reader

	st  ion.Symtab
	tmp []byte

	outlock   sync.Mutex
	writeFail bool
}

var serverPool = sync.Pool{
	New: func() interface{} {
		return &server{}
	},
}

// A Server can be used to serve queries.
type Server struct {
	// Runner is the local execution environment
	// for the query. If Runner is nil, then query
	// execution will fail.
	Runner Runner
	// InitFS is used to initialize a file system
	// which will be used by the Runner to access
	// input objects. The datum passed to InitFS
	// is provided by the client to pass
	// appropriate information necessary to access
	// file system (e.g., credentials).
	InitFS func(ion.Datum) (fs.FS, error)
}

// Serve serves queries from [rw] using [run] to
// run queries.
func Serve(rw io.ReadWriteCloser, run Runner) error {
	s := Server{Runner: run}
	return s.Serve(rw)
}

// Serve serves queries from [rw].
//
// Serve will run until rw.Read returns io.EOF,
// at which point it will return with no error.
// If it encounters an internal error, it will
// close the pipe and return the error.
func (s *Server) Serve(rw io.ReadWriteCloser) error {
	sv := serverPool.Get().(*server)
	sv.run = s.Runner
	sv.initfs = s.InitFS
	sv.pipe = rw
	sv.tmp = sv.tmp[:0]
	sv.writeFail = false
	sv.st.Reset()
	if sv.rd == nil {
		sv.rd = bufio.NewReader(rw)
	} else {
		sv.rd.Reset(rw)
	}
	err := sv.serve()
	serverPool.Put(sv)
	return err
}

func (s *server) frame() (frame, error) {
	buf, err := s.rd.Peek(framesize)
	if err != nil {
		return 0, err
	}
	f := getframe(buf)
	s.rd.Discard(framesize)
	return f, nil
}

func mkframe(kind framekind, size int) frame {
	return frame(uint32(kind<<24) | (uint32(size) & 0xffffff))
}

func (s *server) senderr(text string) error {
	if s.writeFail {
		// don't bother if the client
		// has hung up
		return nil
	}
	if cap(s.tmp) < framesize {
		s.tmp = make([]byte, 0, len(text)+framesize)
	}
	f := mkframe(frameerr, len(text))
	f.put(s.tmp[:framesize])
	s.tmp = append(s.tmp[:framesize], text...)
	_, err := s.pipe.Write(s.tmp)
	return err
}

func (s *server) readn(n int) ([]byte, error) {
	if cap(s.tmp) < n {
		s.tmp = make([]byte, 0, n)
	}
	s.tmp = s.tmp[:n]
	_, err := io.ReadFull(s.rd, s.tmp)
	if err != nil {
		// we're reading data following a frame,
		// so zero bytes of data is never a reasonable
		// amount to return
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return s.tmp, nil
}

// we require Client to keep the pipe open
// for the duration of the query (and write no additional
// data into it); if we get an EOF on the input pipe,
// that means the query is canceled
func (s *server) context() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	r := s.pipe
	go func() {
		defer cancel()
		dst := make([]byte, 1)
		r.Read(dst)
		// don't really care about results here;
		// either we got EOF or we got unexpected client data
	}()
	return ctx, cancel
}

func (s *server) serve() error {
	defer s.pipe.Close()
	f, err := s.frame()
	if err != nil {
		return err
	}
	if f.kind() != framestart {
		s.senderr("unexpected frame")
		return fmt.Errorf("received unexpected frame %x", f)
	}
	buf, err := s.readn(f.length())
	if err != nil {
		s.senderr(err.Error())
		return fmt.Errorf("reading start frame: %w", err)
	}
	ctx, cancel := s.context()
	return s.runQuery(buf, ctx, cancel)
}

// Write implements io.Writer (passed to Exec);
func (s *server) Write(buf []byte) (int, error) {
	if len(buf) > maxframe {
		return 0, fmt.Errorf("server: length %d exceeds framing limit", len(buf))
	}
	s.outlock.Lock()
	defer s.outlock.Unlock()
	if s.writeFail {
		// this is the signal to the
		// core code that we can stop processing
		return 0, io.EOF
	}
	// note: we promote errors here to io.EOF
	// to cause the code on the server side
	// to tear down cleanly; if the client disappears
	// then there's not really much for us to do
	// in terms of error handling
	//
	// this may happen simply because the client
	// is executing a LIMIT and has received
	// enough data
	err := s.writeframe(mkframe(framedata, len(buf)))
	if err != nil {
		s.writeFail = true
		return 0, fmt.Errorf("client disappeared (%s): %w", err, io.EOF)
	}
	n, err := s.pipe.Write(buf)
	if err == nil && n < len(buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		s.writeFail = true
		err = fmt.Errorf("client disappeared (%s): %w", err, io.EOF)
	}
	return n, err
}

func (s *server) writeframe(f frame) error {
	if cap(s.tmp) < framesize {
		s.tmp = make([]byte, framesize)

	}
	s.tmp = s.tmp[:framesize]
	f.put(s.tmp)
	n, err := s.pipe.Write(s.tmp)
	if err == nil && n != len(s.tmp) {
		err = io.ErrShortWrite
	}
	return err
}

func (s *server) fin(stat *ExecStats) error {
	if s.writeFail {
		// writes already failed;
		// don't bother sending a fin
		return nil
	}
	var buf ion.Buffer
	buf.Set(s.tmp[:framesize])
	stat.Encode(&buf, &statsSymtab)
	out := buf.Bytes()
	mkframe(framefin, buf.Size()-framesize).put(out)
	_, err := s.pipe.Write(out)
	return err
}

func (s *server) ctxerr(ctx context.Context, err error) error {
	if ctxerr := ctx.Err(); ctxerr != nil {
		return ctxerr
	}
	return err
}

func (s *server) runQuery(buf []byte, ctx context.Context, cancel func()) error {
	defer cancel()
	s.st.Reset()

	buf, err := s.st.Unmarshal(buf)
	if err != nil {
		s.senderr(err.Error())
		return s.ctxerr(ctx, err)
	}
	t, err := Decode(&s.st, buf)
	if err != nil {
		s.senderr(err.Error())
		return s.ctxerr(ctx, err)
	}
	lp := LocalTransport{}
	ep := ExecParams{
		Plan:    t,
		Output:  s,
		Context: ctx,
		Runner:  s.run,
	}
	if s.initfs != nil && !t.Data.IsEmpty() {
		ep.FS, err = s.initfs(t.Data)
		if err != nil {
			return err
		}
	}
	err = lp.Exec(&ep)
	if err != nil {
		s.senderr(err.Error())
		return s.ctxerr(ctx, err)
	}
	return s.fin(&ep.Stats)
}

// Client represents a connection to a "remote"
// query-processing environment.
//
// A Client can be constructed simply by
// declaring a zero-value Client and then
// assigning the Pipe field to the desired connection.
type Client struct {
	// Pipe is the connection to the
	// remote query environment.
	Pipe io.ReadWriteCloser

	// used for sending query plans
	st  ion.Symtab
	iob ion.Buffer

	// tmp is a scratch buffer
	// used for marshaling requests
	// and reading responses;
	// when processing response frames,
	// the data in tmp[:valid] is data
	// that was returned from calls to Pipe.Read
	// that has not yet been processed
	// (since we allow Read to fill as much
	// of the scratch buffer as it would like)
	tmp   []byte
	valid int
}

var _ Transport = &Client{}

// Exec executes a query across the client connection.
// Exec implements Transport.Exec.
//
// Exec is *not* safe to call from multiple goroutines
// simultaneously.
func (c *Client) Exec(ep *ExecParams) error {
	c.st.Reset()
	c.iob.Reset()
	c.valid = 0
	err := c.send(ep)
	if err != nil {
		return err
	}
	return c.copyout(ep)
}

// Close closes c.Pipe
func (c *Client) Close() error {
	return c.Pipe.Close()
}

func (c *Client) send(ep *ExecParams) error {
	err := ep.Plan.encode(&c.iob, &c.st, ep)
	if err != nil {
		return fmt.Errorf("plan.Client.Exec: encoding plan: %w", err)
	}
	if cap(c.tmp) < framesize {
		c.tmp = make([]byte, framesize)
	}
	stpos := c.iob.Size()
	c.iob.UnsafeAppend(c.tmp[:framesize]) // will frob this later
	c.st.Marshal(&c.iob, true)
	if c.iob.Size()-framesize > maxframe {
		return fmt.Errorf("plan.Client.Exec: encoded query (%d bytes) too large", c.iob.Size())
	}
	first := c.iob.Bytes()[stpos:]
	second := c.iob.Bytes()[:stpos]
	mkframe(framestart, c.iob.Size()-framesize).put(first)
	_, err = c.Pipe.Write(first)
	if err != nil {
		return err
	}
	_, err = c.Pipe.Write(second)
	return err
}

func (c *Client) next() (frame, error) {
	if c.valid < framesize {
		n, err := io.ReadAtLeast(c.Pipe, c.tmp[c.valid:], framesize-c.valid)
		c.valid += n
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			return 0, err
		}
	}
	return getframe(c.tmp), nil
}

// buffer at least the next 'size' bytes of
// input and return them
func (c *Client) buffer(size int) ([]byte, error) {
	// we need to fill tmp with the full
	// frame size, plus the frame header;
	// if we don't have all of that data yet,
	// continue reading until we do
	total := size + framesize
	if total > c.valid {
		if cap(c.tmp) < total {
			nv := make([]byte, total)
			copy(nv, c.tmp[:c.valid])
			c.tmp = nv
		}
		c.tmp = c.tmp[:total]
		n, err := io.ReadFull(c.Pipe, c.tmp[c.valid:])
		c.valid += n
		if n == 0 {
			// io.ReadFull can return io.EOF
			// if exactly zero bytes are read,
			// but in this case we know for certain
			// that we should have read more than
			// zero bytes, so promote the error:
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			return nil, err
		}
	}
	return c.tmp[framesize:total], nil
}

func (c *Client) output(dst io.Writer, size int) error {
	// write out the buffered data and adjust c.valid
	buf, err := c.buffer(size)
	if err != nil {
		return err
	}
	// NOTE: this is triggering a copy operation
	// in vm due to buf not being allocated from vm.Malloc;
	// on balance this is typically fine because we try
	// to make the data sent between nodes very small
	w, err := dst.Write(buf)
	if err != nil {
		return err
	}
	if w != size {
		return fmt.Errorf("io.Write returned %d bytes written instead of %d w/o error?", w, size)
	}
	c.valid -= (w + framesize)
	// if we have any valid bytes remaining,
	// copy them to the front of the buffer
	if c.valid > 0 {
		total := len(buf) + framesize
		copy(c.tmp, c.tmp[total:total+c.valid])
	}
	return nil
}

func (c *Client) queryerr(size int) error {
	var bld strings.Builder
	bld.WriteString("remote error: ")
	err := c.output(&bld, size)
	if err != nil {
		return err
	}
	return errors.New(bld.String())
}

func (c *Client) decodestat(stat *ExecStats, size int) error {
	var tmp ExecStats
	buf, err := c.buffer(size)
	if err != nil {
		return err
	}
	err = tmp.UnmarshalBinary(buf)
	if err != nil {
		return err
	}
	stat.atomicAdd(&tmp)
	return nil
}

func closeOnCancel(pipe io.ReadWriteCloser, cancel, done <-chan struct{}) {
	select {
	case <-cancel:
		pipe.Close()
	case <-done:
	}
}

func (c *Client) copyout(ep *ExecParams) error {
	dst := ep.Output
	stat := &ep.Stats
	var done chan struct{}
	if cancel := ep.Context.Done(); cancel != nil {
		done = make(chan struct{})
		defer func() {
			close(done)
		}()
		go closeOnCancel(c.Pipe, cancel, done)
	}
	for {
		f, err := c.next()
		if err != nil {
			// see if the error was due to cancellation, etc:
			if ctxerr := ep.Context.Err(); ctxerr != nil {
				err = ctxerr
			}
			return fmt.Errorf("plan.Client: reading frame: %w", err)
		}
		switch f.kind() {
		case framefin:
			// done!
			return c.decodestat(stat, f.length())
		case framedata:
			err = c.output(dst, f.length())
			if err != nil {
				// The destination may close the pipe
				// if it is imposing a LIMIT on the
				// number of returned rows.
				if errors.Is(err, io.EOF) {
					return nil
				}
				return fmt.Errorf("plan.Client: writing output: %w", err)
			}
		case frameerr:
			return c.queryerr(f.length())
		default:
			// unexpected frame
			return fmt.Errorf("plan.Client: unexpected frame %x", f)
		}
	}
}
