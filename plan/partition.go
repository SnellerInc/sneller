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
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/ion"
)

// Subtables represents a list of subtables with an
// opaque encoding and in-memory representation.
type Subtables interface {
	// Len returns the number of subtables.
	Len() int
	// Subtable copies the ith Subtable into sub.
	// This may panic if i is out of range.
	// Implementations must not retain sun.
	Subtable(i int, sub *Subtable)
	// Encode encodes the subtables into dst.
	Encode(st *ion.Symtab, dst *ion.Buffer) error
	// Filter applies the given filter expression
	// to every TableHandle in each subtables.
	Filter(expr.Node)
	// Append appends another subtable list to this
	// one. Implementations may assume the argument
	// is always one of the Subtables produced by
	// the same Encoder that produced the receiver.
	Append(Subtables) Subtables
}

// SubtableList is a basic implementation of Subtables.
// This implementation is used if a Decoder does not
// implement DecodeSubtables.
type SubtableList []Subtable

// Len returns the number of subtables.
func (s SubtableList) Len() int {
	return len(s)
}

// Subtable copies the ith Subtable into sub.
func (s SubtableList) Subtable(i int, sub *Subtable) {
	*sub = s[i]
}

// Encode encodes the list into dst.
func (s SubtableList) Encode(st *ion.Symtab, dst *ion.Buffer) error {
	dst.BeginList(-1)
	for i := range s {
		if err := s[i].encode(st, dst); err != nil {
			return err
		}
	}
	dst.EndList()
	return nil
}

// Filter applies the given filter expression to every
// TableHandle in the list.
func (s SubtableList) Filter(e expr.Node) {
	for i := range s {
		if fh, ok := s[i].Handle.(Filterable); ok {
			s[i].Handle = fh.Filter(e)
		}
	}
}

// Append combines this list with another list. The
// argument sub must have concrete type SubtableList.
func (s SubtableList) Append(sub Subtables) Subtables {
	return append(s, sub.(SubtableList)...)
}

// DecodeSubtables decodes a list of Subtable objects
func DecodeSubtables(d Decoder, st *ion.Symtab, body []byte) (Subtables, error) {
	if d, ok := d.(SubtableDecoder); ok {
		return d.DecodeSubtables(st, body)
	}
	var sub SubtableList
	err := unpackList(body, func(field []byte) error {
		body, err := nonemptyList(field)
		if err != nil {
			return err
		}
		t, err := DecodeTransport(st, body)
		if err != nil {
			return err
		}
		body = body[ion.SizeOf(body):]
		e, body, err := expr.Decode(st, body)
		if err != nil {
			return err
		}
		tbl, ok := e.(*expr.Table)
		if !ok {
			return fmt.Errorf("decoding UnionMap: cannot use %T as expr.Table", e)
		}
		var th TableHandle
		if len(body) > 0 && ion.TypeOf(body) != ion.NullType {
			th, err = decodeHandle(d, st, body)
			if err != nil {
				return err
			}
		}
		sub = append(sub, Subtable{
			Transport: t,
			Table:     tbl,
			Handle:    th,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sub, nil
}

// Subtable is a (Transport, Table) tuple
// that is returned by Splitter.Split
// to indicate how queries that access
// particular tables should be split.
//
// See: Splitter.Split
type Subtable struct {
	// Transport is the transport
	// over which the subquery should
	// be executed.
	Transport

	// Table is the expression
	// that the Transport should use
	// as the table value.
	*expr.Table

	Handle TableHandle
}

// encode as [transport, table-expr, handle]
func (s *Subtable) encode(st *ion.Symtab, dst *ion.Buffer) error {
	dst.BeginList(-1)
	err := EncodeTransport(s.Transport, st, dst)
	if err != nil {
		return err
	}
	s.Table.Encode(dst, st)
	if s.Handle == nil {
		dst.WriteNull()
	} else if err := s.Handle.Encode(dst, st); err != nil {
		return err
	}
	dst.EndList()
	return nil
}

// Splitter is the interface through which
// a caller can control how queries are split
// into multiple sub-queries that are executed
// using different Transports.
type Splitter interface {
	// Split takes the table expression
	// from the original query along with
	// the table handle returned by Env.Stat
	// and yields a list of Subtables to
	// be used to evaluate the sub-query
	// in parallel.
	Split(expr.Node, TableHandle) (Subtables, error)
}

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
	pipe io.ReadWriteCloser
	rd   *bufio.Reader
	dec  Decoder

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

// Serve serves queries from the given io.ReadWriter
// using hfn to determine how to handle tables.
//
// Serve will run until rw.Read returns io.EOF,
// at which point it will return with no error.
// If it encounters an internal error, it will
// close the pipe and return the error.
func Serve(rw io.ReadWriteCloser, dec Decoder) error {
	s := serverPool.Get().(*server)
	s.pipe = rw
	if s.rd == nil {
		s.rd = bufio.NewReader(rw)
	} else {
		s.rd.Reset(rw)
	}
	s.dec = dec
	s.st.Reset()
	err := s.serve()
	serverPool.Put(s)
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

func (s *server) serve() error {
	defer s.pipe.Close()
	for {
		f, err := s.frame()
		if err != nil {
			// clean shutdown
			if err == io.EOF {
				return nil
			}
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
		err = s.runQuery(buf)
		if err != nil {
			s.senderr(err.Error())
			// note: we don't close the connection here;
			// we let the client tear down the connection
		}
		if s.writeFail {
			return nil
		}
	}
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
	_, err := s.pipe.Write(s.tmp)
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

func (s *server) runQuery(buf []byte) error {
	s.st.Reset()
	var err error
	var t *Tree

	buf, err = s.st.Unmarshal(buf)
	if err != nil {
		return err
	}
	t, err = Decode(s.dec, &s.st, buf)
	if err != nil {
		return err
	}
	var stat ExecStats
	err = Exec(t, s, &stat)
	if err != nil {
		return err
	}
	return s.fin(&stat)
}

// Client represents a connection to a "remote"
// query-processing environment.
//
// A Client can be constructed simply by
// declaring a zero-value Client and then
// assigning the Pipe field to the desired connection.
//
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

// TableRewrite is a function
// that accepts a table expression
// and returns a new table expression.
type TableRewrite func(*expr.Table, TableHandle) (*expr.Table, TableHandle)

// Exec executes a query across the client connection.
// Exec implements Transport.Exec.
//
// Exec is *not* safe to call from multiple goroutines
// simultaneously.
func (c *Client) Exec(t *Tree, rw TableRewrite, dst io.Writer, stat *ExecStats) error {
	c.st.Reset()
	c.iob.Reset()
	c.valid = 0
	err := c.send(t, rw)
	if err != nil {
		return err
	}
	return c.copyout(dst, stat)
}

// Close closes c.Pipe
func (c *Client) Close() error {
	return c.Pipe.Close()
}

func (c *Client) send(t *Tree, rw TableRewrite) error {
	err := t.EncodePart(&c.iob, &c.st, rw)
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
		if err != nil {
			return 0, fmt.Errorf("plan.Client: reading frame: %w", err)
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

func (c *Client) copyout(dst io.Writer, stat *ExecStats) error {
	for {
		f, err := c.next()
		if err != nil {
			return fmt.Errorf("plan.Client: reading from connection: %w", err)
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
