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

	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/blob"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/ion/blockfmt"
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
	canVMOpen = true

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

	// capture vm errors associated with this tenant
	vm.Errorf = logger.Printf
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

type tenantHandle struct {
	parent *tenantEnv
	inner  plan.TableHandle
}

func (t *tenantEnv) DecodeHandle(st *ion.Symtab, buf []byte) (plan.TableHandle, error) {
	decodeHandle := func(st *ion.Symtab, mem []byte) (plan.TableHandle, error) {
		fh := new(filterHandle)
		if err := fh.decode(st, mem); err != nil {
			return nil, err
		}
		return fh, nil
	}
	h, err := decodeHandle(st, buf)
	if err != nil {
		return nil, err
	}
	return &tenantHandle{parent: t, inner: h}, nil
}

var _ plan.SubtableDecoder = (*tenantEnv)(nil)

// DecodeSubtables implements plan.SubtableDecoder.
func (t *tenantEnv) DecodeSubtables(st *ion.Symtab, buf []byte) (plan.Subtables, error) {
	thfn := func(blobs []blob.Interface, flt expr.Node) plan.TableHandle {
		h := &filterHandle{
			blobs:  &blob.List{Contents: blobs},
			filter: flt,
		}
		return &tenantHandle{parent: t, inner: h}
	}
	return decodeSubtables(st, buf, thfn)
}

var _ plan.UploaderDecoder = (*tenantEnv)(nil)

// DecodeUploader implements plan.UploaderDecoder.
func (t *tenantEnv) DecodeUploader(st *ion.Symtab, buf []byte) (plan.UploadFS, error) {
	if testmode {
		return db.DecodeDirFS(st, buf)
	}
	return db.DecodeS3FS(st, buf)
}

func (e *tenantEnv) post() {
	e.evfd.Write(e.onebuf[:])
}

func (h *tenantHandle) Encode(dst *ion.Buffer, st *ion.Symtab) error {
	return h.inner.Encode(dst, st)
}

func (h *tenantHandle) Open() (vm.Table, error) {
	fh := h.inner.(*filterHandle)
	lst := fh.blobs
	if !canVMOpen {
		panic("shouldn't have called filterHandle.Open()")
	}
	segs := make([]dcache.Segment, 0, len(lst.Contents))
	var flt filter
	if fh.filter != nil {
		if m, ok := compileFilter(fh.filter); ok {
			flt = m
		}
	}
	var size int64
	for i := range lst.Contents {
		if h.parent.HTTPClient != nil {
			blob.Use(lst.Contents[i], h.parent.HTTPClient)
		}
		b := lst.Contents[i]
		if pc, ok := b.(*blob.CompressedPart); ok && flt != nil {
			if scan := maxscan(pc, flt); scan == 0 {
				continue
			}
		}
		seg := &blobSegment{
			blob: b,
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
	return h.parent.cache.MultiTable(segs, flags), nil
}

func (h *tenantHandle) Filter(e expr.Node) plan.TableHandle {
	return &tenantHandle{
		parent: h.parent,
		inner:  h.inner.(*filterHandle).Filter(e),
	}
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
	if c, ok := b.blob.(*blob.CompressedPart); ok {
		// compressed: do decoding
		var dec blockfmt.Decoder
		dec.Set(c.Parent.Trailer, c.EndBlock)
		_, err := dec.CopyBytes(dst, src)
		return err
	}
	if c, ok := b.blob.(*blob.Compressed); ok {
		var dec blockfmt.Decoder
		dec.Set(c.Trailer, len(c.Trailer.Blocks))
		_, err := dec.CopyBytes(dst, src)
		return err
	}
	// default: just write the segments directly
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
