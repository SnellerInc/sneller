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

package tnproto

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan"
)

func init() {
	plan.AddTransportDecoder("remote-tenant", func() plan.TransportDecoder {
		return new(Remote)
	})
}

// Remote is an implementation of plan.Transport
// that asks a remote tenant to execute a query
// using a ProxyExec request.
type Remote struct {
	// ID is the ID of the tenant
	// that we should connect to.
	ID ID

	// Key is the preshared key to authorize
	// requests to the tenant.
	Key Key

	// Net and Addr are the network
	// type and address of the remote
	// connection to make.
	// These arguments are passed
	// verbatim to net.Dial.
	Net, Addr string

	// Timeout, if non-zero, is the
	// dial timeout dialing the
	// remote connection.
	// This argument is passed verbatim
	// to net.DialTimeout; see the caveats
	// in net.DialTimeout for which steps
	// of dialing (like DNS resolution)
	// are part of the timeout window.
	Timeout time.Duration
}

func (r *Remote) SetField(f ion.Field) error {
	var err error
	switch f.Label {
	case "net":
		r.Net, err = f.String()
	case "addr":
		r.Addr, err = f.String()
	case "timeout":
		var i int64
		i, err = f.Int()
		r.Timeout = time.Duration(i)
	case "id":
		var buf []byte
		buf, err = f.BlobShared()
		if err == nil && copy(r.ID[:], buf) != len(r.ID[:]) {
			err = fmt.Errorf("decoding tnproto.Remote: tenant ID should not be %d bytes", len(buf))
		}
	case "key":
		var buf []byte
		buf, err = f.BlobShared()
		if err == nil && copy(r.Key[:], buf) != len(r.Key[:]) {
			err = fmt.Errorf("decoding tnproto.Remote: tenant key should not be %d bytes", len(buf))
		}
	default:
		return fmt.Errorf("decoding tnproto.Remote: unknown field %q", f.Label)
	}

	return err
}

func (r *Remote) Encode(dst *ion.Buffer, st *ion.Symtab) {
	dst.BeginStruct(-1)
	dst.BeginField(st.Intern("type"))
	dst.WriteSymbol(st.Intern("remote-tenant"))
	dst.BeginField(st.Intern("net"))
	dst.WriteString(r.Net)
	dst.BeginField(st.Intern("addr"))
	dst.WriteString(r.Addr)
	dst.BeginField(st.Intern("id"))
	dst.WriteBlob(r.ID[:])
	dst.BeginField(st.Intern("key"))
	dst.WriteBlob(r.Key[:])
	dst.EndStruct()
}

var clientPool = sync.Pool{
	New: func() interface{} {
		return &plan.Client{}
	},
}

// Exec implements plan.Transport.Exec
// by dialing the address given by r.Net and r.Addr
// and sending it an Attach message, followed
// by a single query execution request with
// plan.Client.Exec.
//
// See also: Attach
func (r *Remote) Exec(ep *plan.ExecParams) error {
	dl := net.Dialer{Timeout: r.Timeout}
	conn, err := dl.DialContext(ep.Context, r.Net, r.Addr)
	if err != nil {
		return err
	}
	// tell the tenant manager to attach us
	// to the right tenant instance
	err = Attach(conn, r.ID, r.Key)
	if err != nil {
		conn.Close()
		return err
	}
	// now we should be talking to the tenant itself;
	// just use the plan.Client machinery
	cl := clientPool.Get().(*plan.Client)
	cl.Pipe = conn
	defer func() {
		cl.Close()
		cl.Pipe = nil
		clientPool.Put(cl)
	}()
	return cl.Exec(ep)
}
