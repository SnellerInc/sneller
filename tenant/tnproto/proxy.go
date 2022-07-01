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

// Package tnproto defines the types and functions
// necessary to speak the tenant control protocol.
package tnproto

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httputil"
	"time"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/usock"
)

// OutputFormat selects an output format
// for DirectExec requests.
//
// The actual OutputFormat byte values
// are the ASCII digits starting at '0'
type OutputFormat byte

const (
	// OutputRaw outputs a raw ion data stream
	OutputRaw = '0' + iota
	// OutputChunkedIon outputs an ion data stream
	// using HTTP chunked encoding
	OutputChunkedIon
	// OutputChunkedJSON outputs a JSON data stream
	// using HTTP chunked encoding
	OutputChunkedJSON
	// OutputChunkedJSONArray outputs a single
	// JSON array object using HTTP chunked encoding
	OutputChunkedJSONArray
)

func (o OutputFormat) String() string {
	switch o {
	case OutputRaw:
		return "raw"
	case OutputChunkedIon:
		return "chunked-ion"
	case OutputChunkedJSON:
		return "chunked-json"
	case OutputChunkedJSONArray:
		return "chunked-json-array"
	default:
		return fmt.Sprintf("unknown format %c", byte(o))
	}
}

// note: for HTTP chunking we do not
// write the final "0\r\n\r\n", as it is
// handled by the net/http package when
// the parent's HTTP handler returns,
// hence we do not call http.NewChunkedWriter(...).Close()
func (o OutputFormat) writer(dst io.WriteCloser) io.WriteCloser {
	switch o {
	case OutputRaw:
		return dst
	case OutputChunkedIon:
		return &writerCloser{Writer: httputil.NewChunkedWriter(dst), Closer: dst}
	case OutputChunkedJSON:
		return httpChunkedJSON(dst)
	case OutputChunkedJSONArray:
		return httpJSONArray(dst)
	default:
		panic(fmt.Sprintf("bad output format: %s", o))
	}
}

// RemoteError is the type of error
// returned from operations where
// the remote machine decided to
// write an error response back
// to the client.
type RemoteError struct {
	Text string
}

// Error implements error
func (r *RemoteError) Error() string {
	return r.Text
}

func remote(text string) *RemoteError {
	return &RemoteError{Text: text}
}

var (
	// prologue to establishing a proxy connection
	proxymsg = []byte("proxyme\n")

	// prologue to executing a query directly
	// into a provided file descriptor;
	// the first 4 zero chars are replaced with
	// the length of the message (in binary)
	// and the final char is set to the output format
	directmsg = []byte("dir00000")

	// response from a tenant that the query plan
	// is invalid or couldn't be parsed
	errmsg = []byte("err0000\n")

	// response from a tenant that the query has
	// begun execution and error(s) will be written
	// over the returned pipe
	detachmsg = []byte("detach!\n")
)

// ProxyExec tells the tenant listening on the
// query socket to establish a connection
// over 'conn' for executing remote queries.
//
// ProxyExec performs exactly one Write call
// on ctl, so it is safe to call ProxyExec
// on the same control socket from multiple
// goroutines sumultaneously.
//
// The socket backing 'conn' will be served
// by the tenant using plan.Serve.
// See also: plan.Serve, plan.Client.
func ProxyExec(ctl *net.UnixConn, conn net.Conn) error {
	_, err := usock.WriteWithConn(ctl, proxymsg, conn)
	return err
}

// intermediate serialization state
// for sending DirectExec messages
// (sometimes the plan.Tree contains
// an enormous blob list, so re-using
// this state reduces GC pressure)
type serializer struct {
	mainbuf  ion.Buffer
	stbuf    ion.Buffer
	st       ion.Symtab
	pre      [8]byte
	prepared bool
}

func (s *serializer) prepare(t *plan.Tree, f OutputFormat) error {
	s.prepared = false
	s.stbuf.Reset()
	copy(s.pre[:], directmsg)
	s.pre[7] = byte(f)
	s.stbuf.UnsafeAppend(s.pre[:]) // we will frob this later
	s.mainbuf.Reset()
	s.st.Reset()
	err := t.Encode(&s.mainbuf, &s.st)
	if err != nil {
		return err
	}
	s.st.Marshal(&s.stbuf, true)
	size := uint32(s.mainbuf.Size() + s.stbuf.Size() - 8)
	binary.LittleEndian.PutUint32(s.stbuf.Bytes()[3:], size)
	s.prepared = true
	return nil
}

func (s *serializer) send(ctl *net.UnixConn, conn net.Conn) error {
	if !s.prepared {
		panic("send before prepare")
	}
	// the implementation of Serve is non-blocking,
	// so any considerable delay here means we are
	// ridiculously overloaded or something has gone wrong;
	// in either case we should not block forever
	ctl.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := usock.WriteWithConn(ctl, s.stbuf.Bytes(), conn)
	if err != nil {
		return fmt.Errorf("in DirectExec: usock.WriteWithConn: %w", err)
	}
	_, err = ctl.Write(s.mainbuf.Bytes())
	if err != nil {
		return fmt.Errorf("in DirectExec: writing plan: %w", err)
	}
	ctl.SetWriteDeadline(time.Time{})
	return nil
}

// Buffer maintains temporary state for sending
// DirectExec messages. The zero value of Buffer
// is usable directly. Re-using a Buffer for
// DirectExec calls is encouraged, as it will
// reduce resource utilization.
type Buffer struct {
	serializer
}

// Prepare prepepares a serialized query
// in b. Each call to Prepare overwrites
// the serialized query produced by
// preceding calls to Prepare.
func (b *Buffer) Prepare(t *plan.Tree, f OutputFormat) error {
	return b.prepare(t, f)
}

// DirectExec sends a query plan to a tenant
// over the control socket ctl and requests
// that it write query results into the socket
// represented by conn.
// DirectExec should only be called after
// Buffer.Prepare has been called at least
// once with the query that should be executed.
//
// If the query is launched successfully,
// DirectExec returns an io.ReadCloser that
// can be used to read errors from the query execution.
// If the ReadCloser yields no output before EOF,
// then the query terminated successfully.
// Otherwise, the data returned via the ReadCloser
// will consist of error text describing how the
// query failed to execute.
// Closing the returned ReadCloser before reading
// the response implicitly cancels the query execution.
//
// DirectExec makes multiple calls to read and
// write data via 'ctl', so the caller is required
// to synchronize access to the control socket in
// a reasonable manner to ensure that message exchanges
// are not interleaved.
func (b *Buffer) DirectExec(ctl *net.UnixConn, conn net.Conn) (io.ReadCloser, error) {
	if !b.prepared {
		return nil, fmt.Errorf("call to tnproto.Buffer.DirectExec before tnproto.Buffer.Prepare")
	}
	err := b.send(ctl, conn)
	if err != nil {
		return nil, err
	}

	// the child can respond with either
	// errnow() or detach()
	ctl.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, errpipe, err := usock.ReadWithFile(ctl, b.pre[:])
	ctl.SetReadDeadline(time.Time{})
	if err != nil {
		return nil, fmt.Errorf("in DirectExec: usock.ReadWithConn: %w", err)
	}
	// ordinary case: everything is fine,
	// please take the error pipe
	if bytes.Equal(b.pre[:], detachmsg) {
		if errpipe == nil {
			return nil, fmt.Errorf("got detach message but no error pipe?")
		}
		return errpipe, nil
	}
	// in-band error case
	if bytes.Equal(b.pre[:3], errmsg[:3]) && b.pre[7] == '\n' {
		if errpipe != nil {
			// shouldn't happen...
			errpipe.Close()
		}
		errlen := int(binary.LittleEndian.Uint32(b.pre[3:]))
		errbuf := make([]byte, errlen)
		ctl.SetReadDeadline(time.Now().Add(1 * time.Second))
		_, err = io.ReadFull(ctl, errbuf)
		ctl.SetReadDeadline(time.Time{})
		if err != nil {
			return nil, fmt.Errorf("DirectExec: reading error response: %w", err)
		}
		return nil, remote(string(errbuf))
	}
	if errpipe != nil {
		errpipe.Close()
	}
	return nil, fmt.Errorf("unexpected tenant response %q", b.pre[:])
}

// Serve responds to ProxyExec and DirectExec requests
// over the given control socket.
func Serve(ctl *net.UnixConn, dec plan.Decoder) error {
	var msgbuf [8]byte
	var st ion.Symtab
	var tmp []byte
	for {
		n, conn, err := usock.ReadWithConn(ctl, msgbuf[:])
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("tnproto.Serve: ReadWithConn: %w", err)
		}
		if conn == nil {
			return fmt.Errorf("expected a control socket, but found none...?")
		}
		if n != len(msgbuf[:]) {
			if conn != nil {
				conn.Close()
			}
			return fmt.Errorf("control message only %d bytes?", n)
		}
		if bytes.Equal(msgbuf[:], proxymsg) {
			// proxy request
			go serveProxy(dec, conn)
		} else if bytes.Equal(msgbuf[:3], directmsg[:3]) {
			// need to read the plan
			// and then execute it directly
			ofmt := OutputFormat(msgbuf[7])
			size := int(binary.LittleEndian.Uint32(msgbuf[3:]))
			if cap(tmp) < size {
				tmp = make([]byte, size)
			}
			// DirectExec writes the message in
			// a single sendmsg(2) call, so if
			// we encounter any significant delay here,
			// something has gone terribly wrong
			ctl.SetReadDeadline(time.Now().Add(time.Second))
			tmp = tmp[:size]
			_, err := io.ReadFull(ctl, tmp)
			ctl.SetReadDeadline(time.Time{}) // clear deadline
			if err != nil {
				return fmt.Errorf("tnproto.Serve: reading DirectExec message: %w", err)
			}
			st.Reset()
			tmp, err = st.Unmarshal(tmp)
			if err != nil {
				return fmt.Errorf("tnproto.Serve: decoding symbol table: %w", err)
			}
			t, err := plan.Decode(dec, &st, tmp)
			if err != nil {
				err = errnow(ctl, err, tmp)
				if err != nil {
					return err
				}
			} else {
				errorWriter, err := detach(ctl)
				if err != nil {
					return err
				}
				go serveDirect(t, ofmt.writer(conn), errorWriter)
			}
		} else {
			if conn != nil {
				conn.Close()
			}
			return fmt.Errorf("unhandled control message %q", msgbuf[:])
		}
	}
}

func serveProxy(dec plan.Decoder, conn net.Conn) {
	defer conn.Close()
	plan.Serve(conn, dec)
}

// pipectx returns a context.Context that is canceled
// when the pipe is closed
func pipectx(errpipe net.Conn) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	go func() {
		defer cancel()
		var buf [1]byte
		dl, _ := ctx.Deadline()
		// make sure that the call to Read returns
		// no later than the cancellation deadline:
		errpipe.SetReadDeadline(dl)
		errpipe.Read(buf[:])
		// either we are already canceled (errpipe closed)
		// or we got EOF (and should cancel) or we got
		// unexpected data (and should probably still cancel)
	}()
	return ctx
}

func sendError(conn io.WriteCloser, err error) {
	var st ion.Symtab
	var buf ion.Buffer

	// send
	//   query_error::{error_message: "..."}
	errsym := st.Intern("query_error")
	message := st.Intern("error_message")
	st.Marshal(&buf, true)
	buf.BeginAnnotation(1)
	buf.BeginField(errsym)
	buf.BeginStruct(-1)
	buf.BeginField(message)
	buf.WriteString(err.Error())
	buf.EndStruct()
	buf.EndAnnotation()

	conn.Write(buf.Bytes())
}

func serveDirect(t *plan.Tree, conn io.WriteCloser, errpipe net.Conn) {
	defer errpipe.Close() // cancels ctx
	ctx := pipectx(errpipe)

	// if we encounter a panic, we don't
	// want to close the errpipe with no output;
	// instead, just write a notification
	// that we are going to panic before we actually
	// do it...
	var outbuf ion.Buffer
	defer func() {
		if e := recover(); e != nil {
			conn.Close()
			outbuf.Reset()
			outbuf.WriteString("panic!")
			errpipe.Write(outbuf.Bytes())
			// re-panic
			panic(e)
		}
	}()
	pl := plan.LocalTransport{}
	ep := plan.ExecParams{
		Output:  conn,
		Context: ctx,
	}
	err := pl.Exec(t, &ep)
	if err != nil {
		sendError(conn, err)
	}
	// must close the connection before
	// indicating the query status to the caller
	conn.Close()
	if err != nil {
		outbuf.WriteString(err.Error())
	} else {
		ep.Stats.Marshal(&outbuf)
	}
	errpipe.Write(outbuf.Bytes())
}

// inside the tenant process,
// indicate that we encountered an error
// while unpacking the query plan
func errnow(ctl *net.UnixConn, err error, tmp []byte) error {
	str := err.Error()
	tmp = append(tmp[:0], errmsg...)
	tmp = append(tmp, str...)
	binary.LittleEndian.PutUint32(tmp[3:], uint32(len(str)))
	ctl.SetWriteDeadline(time.Now().Add(1 * time.Second))
	_, err = ctl.Write(tmp)
	return err
}

// inside the tenant process,
// indicate that we unpacked the plan
// and have begun execution; use the returned
// error pipe for receiving out-of-band error
// notifications
func detach(ctl *net.UnixConn) (net.Conn, error) {
	r, w, err := usock.SocketPair()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	_, err = usock.WriteWithConn(ctl, detachmsg, r)
	if err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

type writerCloser struct {
	io.Writer
	io.Closer
}

func httpChunkedJSON(dst io.WriteCloser) io.WriteCloser {
	return &writerCloser{
		Writer: ion.NewJSONWriter(httputil.NewChunkedWriter(dst), '\n'),
		Closer: dst,
	}
}

type arrayWriter struct {
	*ion.JSONWriter
	final io.Closer
}

func httpJSONArray(dst io.WriteCloser) io.WriteCloser {
	return &arrayWriter{
		JSONWriter: ion.NewJSONWriter(httputil.NewChunkedWriter(dst), ','),
		final:      dst,
	}
}

func (a *arrayWriter) Close() error {
	err := a.JSONWriter.Close()
	err2 := a.final.Close()
	if err == nil {
		err = err2
	}
	return err
}
