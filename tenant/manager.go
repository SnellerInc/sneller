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

// Package tenant encapsulates the
// logic and protocol-level details
// for managing tenant sub-processes.
//
// Background:
//
// In the interest of reducing the blast radius
// of bugs in the core query execution assembly code,
// we're executing queries in separate subprocesses,
// one for each "tenant."
// This ought to keep an out-of-bounds read bug from
// turning into cross-tenant information disclosure.
// However, we need to endure some unfortunate
// incidental complexity in order to make this strategy
// performant. In particular, we're trying our best to
// avoid having either one of the Go processes
// (or the kenel) from having to copy data back and forth
// between pipes, etc., since the query processing
// pipeline would like to consume as much of our memory
// bandwidth as we can give it.
//
// In order to avoid copies, each of the tenant processes
// is launched (lazily!) with a unix control socket over
// which we can pass it output file descriptors.
// So, every request to execute a query will be passed
// over the control socket *along with* the file descriptor
// to which the query output should be written.
//
// There are two sorts of requests that can be
// made to a tenant:
//
//   - "Direct Execution" requests, which are produced
//     by Mangager.Do, provide a query plan to
//     a specific local tenant process, along with an output
//     file descriptor for the tenant to write to.
//     The tenant performs the query and writes the results
//     to the file descriptor.
//
//   - "Proxy Execution" requests, which are produced
//     by tenant processes themselves, provide the ability
//     for a tenant on one machine to "fan out" a query to
//     multiple tenant processes across different machines.
//     These are made available through Manager.Remote
//     (see also: tnproto.Remote).
//
// In practice, "split" query plans (ones that are meant
// to run on multiple machines) will use both of the
// request types above. The original ("reduction") query
// plan will be launched via a Direct Execution request,
// and then its constituent ("mapping") sub-queries will
// be requested by the tenant process via Proxy Execution
// requests.
package tenant

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/tenant/tnproto"
	"github.com/SnellerInc/sneller/usock"
)

const (
	DefaultReapInterval = time.Hour
)

// Manager manages the state associated
// with multiple tenant processes.
// Manager can be used to talk to arbitrary
// tenant processes, and it takes care of lazily
// launching and killing tenant processes based
// on their utilization.
//
// Broadly speaking, the tenant manager has
// two similar responsibilities:
//
//  1. Allow *this* process to talk to
//     a sub-process tenant directly.
//  2. Allow other tenants inside other tenant
//     managers to connect to the tenants running
//     locally via Manager.Remote.
type Manager struct {
	// CacheDir is the root of the directory
	// tree used for caching data.
	CacheDir string
	// Sandbox determines if Manager
	// launches the tenant process
	// with bwrap(1)
	Sandbox bool

	// remote is the socket on which to
	// listen for remote connections
	// from Manager.Serve
	remote net.Listener

	// execPath is the executable path
	// of the tenant binary.
	//
	// The tentant binary is launched as
	//   <execPath> <execArgs ...> tenant-id control-fd
	// where tenant-id is the base64-encoded
	// tenant ID, and control-fd is the file descriptor
	// of the control unix socket that is used
	// to send control messages.
	execPath string

	// execArgs are any args passed to
	// the tenant binary before the tenant id
	// and the control socket file descriptor.
	execArgs []string

	// envfn, if non-nil, is a function
	// used to populate the tenant
	// environment when it is launched
	envfn func(cache string, id tnproto.ID) []string

	// execStderr should return the stderr attached
	// to each of the exec'd children.
	// If ExecStderr is nil, then the children's stderr
	// is attached to the parent's stderr.
	execStderr func(id tnproto.ID) io.Writer

	// gcInterval is the interval at which
	// processes that have been inactive for
	// an extended period of time will be killed.
	gcInterval time.Duration

	lastSummary time.Time

	// TODO: offer a low- and high-water-mark
	// for subprocess reaping. Ideally we have
	// some soft and hard limits in terms of the
	// number of subprocesses we launch.

	// logger is the output to which
	// proxy error messages are logged.
	// If logger is nil, no output is logged.
	logger *log.Logger

	done chan struct{}
	lock sync.Mutex // guards live
	live map[tnproto.ID]*child

	eventfd *os.File

	// candidates for cached files to
	// be evicted when a child process
	// indicates that it is filling a
	// cache entry
	eheap evictHeap

	// when the manager is started,
	// clean 100% of the cache and
	// create a fresh directory,
	// and start the garbage collection
	// routines for tenant processes and
	// cache directories
	initOnce sync.Once

	// warn about being unable to sandbox exactly once
	warnOnce sync.Once
}

// Option is an optional argument
// to NewManager to indicate optional
// Manager configuration.
type Option func(m *Manager)

// WithGCInterval is an option that can
// be passed to NewManager to indicate
// the desired process GC interval.
//
// If interval is zero, then process GC is disabled.
func WithGCInterval(interval time.Duration) Option {
	return func(m *Manager) {
		m.gcInterval = interval
	}
}

const DefaultCacheDir = "/tmp/tenant-cache"

// DefaultEnv is the default
// environment-generating function
// for the tenant process.
// The tenant process should not
// receive all of the parent's environment
// variables, as the parent may have credentials
// stored there.
//
// The cache argument contains the
// return value of filepath.Join(CacheDir, id.String())
// for the CacheDir configured in the manager
// (see Manager.CacheDir).
//
// It sets the following:
//
//	PATH=$PATH
//	SHELL=$SHELL
//	HOME=$HOME
//	LANG=C.UTF-8
//	CACHEDIR=<cache>
func DefaultEnv(cache string, id tnproto.ID) []string {
	x := []string{
		"LANG=C.UTF-8",
		"CACHEDIR=" + cache,
	}
	for _, evar := range []string{
		"PATH", "SHELL", "LANG", "HOME",
	} {
		if val := os.Getenv(evar); val != "" {
			x = append(x, fmt.Sprintf("%s=%s", evar, val))
		}
	}
	return x
}

// WithTenantEnv overrides default tenant
// environment function (see DefaultEnv)
// with another function.
func WithTenantEnv(fn func(string, tnproto.ID) []string) Option {
	return func(m *Manager) {
		m.envfn = fn
	}
}

// WithTenantStderr is an option that
// can be passed to NewManager to indicate
// where the stdout+stderr of tenant processes
// should be directed. The io.Writer produced
// by the function will be passed directly
// to exec.Cmd.Stdout and exec.Cmd.Stderr
//
// See also: exec.Cmd
func WithTenantStderr(fn func(tnproto.ID) io.Writer) Option {
	return func(m *Manager) {
		m.execStderr = fn
	}
}

// WithRemote is an option that can
// be passed to NewManager to indicate
// the listener on which to serve
// remote proxy exec messages.
func WithRemote(l net.Listener) Option {
	return func(m *Manager) {
		m.remote = l
	}
}

// WithLogger is an option that
// can be passed to NewManager to
// have it log diagnostic information.
// If no logger is set for the manager,
// it will not write out any diagnostics.
func WithLogger(l *log.Logger) Option {
	return func(m *Manager) {
		m.logger = l
	}
}

// NewManager makes a new Manager from the
// list of command-line arguments provided
// and the list of additional options.
//
// The cmd list should contain at least one
// element, since the first element of the
// list indicates the name of the program to run.
func NewManager(cmd []string, opt ...Option) *Manager {
	m := &Manager{
		done:       make(chan struct{}),
		execPath:   cmd[0],
		execArgs:   cmd[1:],
		gcInterval: DefaultReapInterval,
		envfn:      DefaultEnv,
		CacheDir:   DefaultCacheDir,
	}
	for i := range opt {
		opt[i](m)
	}
	return m
}

func (m *Manager) init() {
	m.initOnce.Do(func() {
		err := m.clean(m.CacheDir)
		if err != nil {
			m.errorf("cleaning cache dir: %s", err)
		}
		m.eventfd, err = eventfd()
		if err != nil {
			m.errorf("eventfd: %s", err)
		}

		go m.gc()
		go m.cachegc()
	})
}

type child struct {
	avail   chan struct{}
	proc    *os.Process
	ctl     *net.UnixConn
	touched time.Time
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return &tnproto.Buffer{}
	},
}

func (c *child) lock() bool {
	// try fast-path
	select {
	case <-c.avail:
		return true
	default:
	}
	// try again, but block for up to 1 second
	t := time.NewTimer(time.Second)
	defer t.Stop()
	select {
	case <-c.avail:
		return true
	case <-t.C:
		return false
	}
}

func (c *child) unlock() {
	select {
	case c.avail <- struct{}{}:
	default:
		panic("unlock of unlocked child")
	}
}

// ErrOverloaded can be returned by
// calls to Manager.Do when
// too many calls to Do are
// currently pending for the same tenant.
var ErrOverloaded = errors.New("child overloaded")

func (c *child) directExec(t *plan.Tree, ofmt tnproto.OutputFormat, conn net.Conn) (io.ReadCloser, error) {
	buf := bufPool.Get().(*tnproto.Buffer)
	err := buf.Prepare(t, ofmt)
	if err != nil {
		return nil, err
	}
	if !c.lock() {
		return nil, ErrOverloaded
	}
	defer c.unlock()
	ret, err := buf.DirectExec(c.ctl, conn)
	bufPool.Put(buf)
	return ret, err
}

func (c *child) proxyExec(peer net.Conn) error {
	if !c.lock() {
		return ErrOverloaded
	}
	defer c.unlock()
	return tnproto.ProxyExec(c.ctl, peer)
}

// background goroutine launched for each child;
// responsible for calling os.Process.Wait()
// and then removing it from the manager map
//
// NOTE: *nothing else* should call c.Wait();
// otherwise this call to Wait() can panic
func (m *Manager) reap(c *child, id tnproto.ID) {
	// proc.Wait should never return an error
	// unless the child wasn't started, etc.
	state, err := c.proc.Wait()
	if err != nil {
		panic(err)
	}
	_ = state // TODO: examine state
	m.lock.Lock()
	// only delete this child if it
	// precisely the same child instance
	// that we want to reap; otherwise
	// we will race with re-launching
	//
	// when launch() is called it cleans
	// and re-creates the cache directory,
	// so it's fine if we ended up racing
	// and don't remove the cache directory here
	if m.live != nil && m.live[id] == c {
		delete(m.live, id)
		os.RemoveAll(m.cacheDir(id))
	}
	m.lock.Unlock()
	// the Close may race with a send,
	// but that should just cause an EPIPE
	// error to appear on the sender's side
	c.ctl.Close()
}

func (m *Manager) cachegc() {
	var buf [8]byte
	for {
		// each time the client program
		// wants to create a new cache entry,
		// it calls write() to increment the counter;
		// that will cause us to run a sweep to remove
		// old cache entries
		m.cacheEvict()
		_, err := m.eventfd.Read(buf[:])
		if err != nil {
			// expected when m.eventfd.Close() is called elsewhere
			return
		}
	}
}

// "garbage collection" -- kill
func (m *Manager) gc() {
	interval := m.gcInterval
	if interval == 0 {
		return // no gc
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// TODO: if the number of tenant processes
	// is really large, we should probably use
	// a heap here instead so that we can efficiently
	// determine the oldest process
	for {
		select {
		case <-ticker.C:
			m.lock.Lock()
			for id, c := range m.live {
				if time.Since(c.touched) >= interval {
					c.proc.Kill()
					delete(m.live, id)
					os.RemoveAll(m.cacheDir(id))
				}
			}
			m.lock.Unlock()
		case <-m.done:
			return
		}
	}
}

// Serve accepts connections from
// m.Remote in a loop and launches
// a goroutine to service each request.
// It does not return unless m.Stop is called
// or it encounters a permanent error from
// m.Remote.Accept
func (m *Manager) Serve() error {
	m.init()
	for {
		conn, err := m.remote.Accept()
		if err != nil {
			if _, ok := <-m.done; !ok {
				// m.done was closed
				return nil
			}
			return err
		}
		go m.handleRemote(conn)
	}
}

func (m *Manager) cacheDir(id tnproto.ID) string {
	return filepath.Join(m.CacheDir, id.String())
}

func (m *Manager) clean(dir string) error {
	err := os.RemoveAll(dir)
	if err != nil {
		return err
	}
	return os.Mkdir(dir, 0750)
}

func (m *Manager) launch(id tnproto.ID) (*child, error) {
	// make sure the tenant's cache directory
	// is created and empty
	// (we are doing this under m.lock, so
	// we shouldn't be racing against anything else)
	if err := m.clean(m.cacheDir(id)); err != nil {
		return nil, err
	}
	local, remote, err := usock.SocketPair()
	if err != nil {
		return nil, err
	}
	defer remote.Close()
	fd, err := remote.File()
	if err != nil {
		local.Close()
		return nil, err
	}
	// we don't need to keep the remote fd
	// open, since it is connected to the local fd
	defer fd.Close()

	// TODO: sandbox the query process.
	// We can use a tool like bwrap(1) to make most
	// of the filesystem and other pids invisible
	// to the child process. We can also stick it
	// in its own cgroup if we want to limit its
	// memory and CPU use as well.
	//
	// the first file descriptor in exec.Cmd.ExtraFiles
	// is always "3", so we pass that as the argument
	// immediately following the tenant id
	arglist := append(m.execArgs, "-t", id.String(), "-c", "3", "-e", "4")
	var cmd *exec.Cmd
	if m.Sandbox && CanSandbox() {
		cmd = bubblewrap(append([]string{m.execPath}, arglist...), m.cacheDir(id))
	} else {
		if m.Sandbox {
			m.warnOnce.Do(func() {
				m.errorf("warning: bwrap(1) unavailable even though Manager.Sandbox is set!")
			})
		}
		cmd = exec.Command(m.execPath, append(m.execArgs, "-t", id.String(), "-c", "3", "-e", "4")...)
	}
	// note: sandboxing will override
	cmd.Env = m.envfn(m.cacheDir(id), id)
	cmd.Stdin = nil
	if m.execStderr == nil {
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
	} else {
		w := m.execStderr(id)
		cmd.Stdout = w
		cmd.Stderr = w
	}
	cmd.ExtraFiles = []*os.File{fd, m.eventfd}

	// TODO: populate cmd.Stdout, cmd.Stderr
	// so that logs go to the right place
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	avail := make(chan struct{}, 1)
	avail <- struct{}{}
	return &child{
		avail:   avail,
		proc:    cmd.Process,
		ctl:     local,
		touched: time.Now(),
	}, nil
}

// get acquires the handle to a child process,
// exec-ing the tenant associated with 'id'
// if it has not been started yet
func (m *Manager) get(id tnproto.ID) (*child, error) {
	// make sure background processes are initialized
	m.init()
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.live != nil {
		c, ok := m.live[id]
		if ok {
			c.touched = time.Now()
			return c, nil
		}
	}
	c, err := m.launch(id)
	if err != nil {
		return nil, err
	}
	if m.live == nil {
		m.live = make(map[tnproto.ID]*child)
	}
	m.live[id] = c
	go m.reap(c, id)
	return c, nil
}

// Do sends a DirectExec message to
// the given tenant ID managed by m.
// If the tenant process has not been
// started yet, it is launched lazily.
//
// Do may return ErrOverloaded if many
// calls to Do for the same tenant ID are
// outstanding simultaneously.
// (The current implementation determines
// this by bailing out of acquisition of
// the child lock after 1 second of inactivity.)
//
// Once Do returns, the query has begun execution.
// The returned io.ReadCloser will indicate when
// the query has completed execution and if any
// errors were encountered. (Use Check to block
// on query execution and check the final error
// status.)
//
// The result of the query is executed into
// the socket backing the provided net.Conn.
// It is the caller's responsibility to close
// the provided connection. (Note that sending
// the socket over to the tenant subprocess
// involves creating a duplicated file handle,
// so closing 'into' immediately after a call
// to Do will not close the connection from
// the perspective of the tenant process.)
func (m *Manager) Do(id tnproto.ID, t *plan.Tree, ofmt tnproto.OutputFormat, into net.Conn) (io.ReadCloser, error) {
	c, err := m.get(id)
	if err != nil {
		return nil, err
	}
	return c.directExec(t, ofmt, into)
}

// Quit sends a SIGQUIT to the tenant process
// with the provided ID. Quit returns true
// if the signal was sent successfully,
// or false if the signal could not be sent
// (either because no such tenant was running
// or because signal(2) failed).
func (m *Manager) Quit(id tnproto.ID) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.live == nil {
		return false
	}
	c, ok := m.live[id]
	return ok && c.proc.Signal(syscall.SIGQUIT) == nil
}

// Check checks the return status of the
// tenant error pipe returned from Manager.Do.
// Check blocks until the other end of the pipe
// has been closed, and then closes this end of the pipe.
func Check(rc io.ReadCloser, stats *plan.ExecStats) error {
	defer rc.Close()
	msg, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	if len(msg) == 0 {
		return &tnproto.RemoteError{Text: "tenant crashed"}
	}
	if ion.TypeOf(msg) == ion.StringType {
		str, _, err := ion.ReadString(msg)
		if err == nil {
			return &tnproto.RemoteError{Text: str}
		}
		return &tnproto.RemoteError{Text: "(malformed error response)"}
	}
	err = stats.UnmarshalBinary(msg)
	if err == nil {
		return nil
	}
	return &tnproto.RemoteError{Text: "(malformed OK response)"}
}

func (m *Manager) errorf(msg string, args ...interface{}) {
	if m.logger != nil {
		m.logger.Printf(msg, args...)
	}
}

// handle a remote connection that is requesting
// some part of a query be executed for a particular
// tenant on *this* machine
func (m *Manager) handleRemote(conn net.Conn) {
	defer conn.Close()
	id, err := tnproto.ReadID(conn)
	if err != nil {
		m.errorf("connection: %s", err)
		return
	}
	if id.IsZero() {
		return // ping message; just expecting a Close()
	}
	c, err := m.get(id)
	if err != nil {
		m.errorf("couldn't spawn %x: %s", id, err)
		return
	}
	err = c.proxyExec(conn)
	if err != nil {
		m.errorf("id %s: proxy-exec: %s", id, err)
	}
}

// Stop performs a graceful cleanup
// of all of the tenant manager subprocesses.
//
// Calling Stop more than exactly once will
// cause a panic.
func (m *Manager) Stop() {
	if m.remote != nil {
		m.remote.Close()
	}
	close(m.done)
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, c := range m.live {
		c.proc.Kill()
		c.ctl.Close()
	}
	m.live = nil
	if m.eventfd != nil {
		m.eventfd.Close()
		m.eventfd = nil
	}
}
