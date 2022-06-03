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
	plan.AddTransportDecoder("remote-tenant", decodeRemote)
}

// Remote is an implementation of plan.Transport
// that asks a remote tenant to execute a query
// using a ProxyExec request.
type Remote struct {
	// Tenant is the ID of the tenant
	// that we should connect to.
	Tenant ID

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

// callback for decoding remote transports
func decodeRemote(st *ion.Symtab, fields []byte) (plan.Transport, error) {
	out := new(Remote)
	var buf []byte
	var sym ion.Symbol
	var err error
	var i int64
	for len(fields) > 0 {
		sym, fields, err = ion.ReadLabel(fields)
		if err != nil {
			return nil, err
		}
		switch st.Get(sym) {
		case "net":
			out.Net, fields, err = ion.ReadString(fields)
		case "addr":
			out.Addr, fields, err = ion.ReadString(fields)
		case "timeout":
			i, fields, err = ion.ReadInt(fields)
			out.Timeout = time.Duration(i)
		case "id":
			buf, fields, err = ion.ReadBytesShared(fields)
			if err == nil && copy(out.Tenant[:], buf) != len(out.Tenant[:]) {
				err = fmt.Errorf("decoding tnproto.Remote: tenant ID should not be %d bytes", len(buf))
			}
		default:
			fields = fields[ion.SizeOf(fields):]
		}
		if err != nil {
			return nil, err
		}
	}
	return out, nil
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
	dst.WriteBlob(r.Tenant[:])
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
func (r *Remote) Exec(t *plan.Tree, ep *plan.ExecParams) error {
	var conn net.Conn
	var err error
	if r.Timeout != 0 {
		conn, err = net.DialTimeout(r.Net, r.Addr, r.Timeout)
	} else {
		conn, err = net.Dial(r.Net, r.Addr)
	}
	if err != nil {
		return err
	}
	// tell the tenant manager to attach us
	// to the right tenant instance
	err = Attach(conn, r.Tenant)
	if err != nil {
		return err
	}
	// now we should be talking to the tenant itself;
	// just use the plan.Client machinery
	cl := clientPool.Get().(*plan.Client)
	cl.Pipe = conn
	defer func() {
		cl.Close()
		clientPool.Put(cl)
	}()
	return cl.Exec(t, ep)
}
