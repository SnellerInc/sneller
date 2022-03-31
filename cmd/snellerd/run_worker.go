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
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/dcache"
	"github.com/SnellerInc/sneller/tenant/tnproto"
	"github.com/SnellerInc/sneller/vm"
)

// Blob segments will not be cached if the total scan
// size of a request in bytes exceeds this limit.
var cacheLimit = memTotal / 2

func nfds() int {
	d, _ := os.ReadDir("/proc/self/fd")
	return len(d) - 1
}

func runWorker(args []string) {
	workerCmd := flag.NewFlagSet("worker", flag.ExitOnError)
	workerTenant := workerCmd.String("t", "", "tenant identifier")
	workerControlSocket := workerCmd.Int("c", -1, "control socket")
	eventfd := workerCmd.Int("e", -1, "eventfd")
	if workerCmd.Parse(args) != nil {
		os.Exit(1)
	}

	if *workerControlSocket == -1 {
		panic("no control socket file descriptor")
	}
	if *workerTenant == "" {
		panic("unknown tenant")
	}
	if *eventfd == -1 {
		panic("no eventfd passed")
	}
	logger := log.New(os.Stderr, "tid:"+*workerTenant+" ", 0)

	start := nfds()
	defer func() {
		http.DefaultClient.CloseIdleConnections()
		end := nfds()
		if end > start {
			logger.Printf("warning: file descriptor leak: exiting with %d > %d", end, start)
		}
	}()
	f := os.NewFile(uintptr(*workerControlSocket), "<ctlsock>")
	conn, err := net.FileConn(f)
	if err != nil {
		panic(err)
	}
	f.Close()
	uc, ok := conn.(*net.UnixConn)

	evfd := os.NewFile(uintptr(*eventfd), "eventfd")

	if !ok {
		panic(fmt.Errorf("unexpected fd type %T", conn))
	}
	defer uc.Close()

	env := tenantEnv{
		evfd:   evfd,
		Tenant: *workerTenant,
	}
	binary.LittleEndian.PutUint64(env.onebuf[:], 1)
	if cachedir := os.Getenv("CACHEDIR"); cachedir != "" {
		info, err := os.Stat(cachedir)
		if err != nil || !info.IsDir() {
			logger.Printf("ignoring invalid cache dir %s", cachedir)
		} else {
			env.cache = dcache.New(cachedir, env.post)
			env.cache.Logger = logger
		}
	}
	err = tnproto.Serve(uc, &env)
	if err != nil {
		logger.Fatalf("cannot serve: %v", err)
	}
}

type tenantEnv struct {
	Tenant     string
	HTTPClient *http.Client
	evfd       *os.File
	cache      *dcache.Cache
	onebuf     [8]byte
}

func (e *tenantEnv) post() {
	e.evfd.Write(e.onebuf[:])
}

func (e *tenantEnv) Stat(tbl *expr.Table) (plan.TableHandle, error) {
	return &tableHandle{
		tbl: tbl,
		env: e,
	}, nil
}

func (e *tenantEnv) Schema(tbl *expr.Table) expr.Hint {
	// TODO???
	return nil
}

type tableHandle struct {
	tbl *expr.Table
	env *tenantEnv
	flt expr.Node
}

func (h *tableHandle) Open() (vm.Table, error) {
	lst, ok := h.tbl.Value.(*blob.List)
	if !ok {
		return nil, fmt.Errorf("unrecognized table.Value %T", h.tbl.Value)
	}
	segs := make([]dcache.Segment, 0, len(lst.Contents))
	var flt filter
	if h.flt != nil {
		flt = compileFilter(h.flt)
	}
	var size int64
	for i := range lst.Contents {
		if h.env.HTTPClient != nil {
			blob.Use(lst.Contents[i], h.env.HTTPClient)
		}
		blob := lst.Contents[i]
		if flt != nil {
			blob, _ = filterBlob(blob, flt, 0)
			if blob == nil {
				continue
			}
		}
		seg := &blobSegment{
			blob: blob,
		}
		// make sure info can be populated successfully
		s, err := seg.stat()
		if err != nil {
			return nil, err
		}
		segs = append(segs, seg)
		size += s.Size
	}
	if len(segs) == 0 {
		return emptyTable{}, nil
	}
	var flags dcache.Flag
	if cacheLimit > 0 && size > cacheLimit {
		flags = dcache.FlagNoFill
	}
	return h.env.cache.MultiTable(segs, flags), nil
}

func (h *tableHandle) Filter(e expr.Node) plan.TableHandle {
	th := *h
	th.flt = e
	return &th
}

type emptyTable struct{}

func (emptyTable) Chunks() int { return 0 }

func (emptyTable) WriteChunks(dst vm.QuerySink, parallel int) error {
	w, err := dst.Open()
	if err != nil {
		return err
	}
	return w.Close()
}

// blobSegment implements dcache.Segment
type blobSegment struct {
	blob blob.Interface
	info *blob.Info
}

func (b *blobSegment) stat() (*blob.Info, error) {
	if b.info != nil {
		return b.info, nil
	}
	info, err := b.blob.Stat()
	if err != nil {
		return nil, err
	}
	b.info = info
	return info, nil
}

// Size is currently the blob size
func (b *blobSegment) Size() int64 { return b.info.Size }

// ETag implements dcache.Segment.ETag
func (b *blobSegment) ETag() string { return b.info.ETag }

// Read implements dcache.Segment.Open
func (b *blobSegment) Open() (io.ReadCloser, error) {
	return b.blob.Reader(0, b.info.Size)
}

// Decode implements dcache.Segment.Decode
func (b *blobSegment) Decode(dst io.Writer, src []byte) error {
	for off := int64(0); off < b.info.Size; off += int64(b.info.Align) {
		mem := src[off:]
		if len(mem) > b.info.Align {
			mem = mem[:b.info.Align]
		}
		_, err := dst.Write(mem)
		if err != nil {
			return err
		}
	}
	return nil
}
