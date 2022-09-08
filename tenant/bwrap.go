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

package tenant

import (
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"

	"github.com/SnellerInc/sneller/cgroup"
)

var (
	bwrap     string
	bwrapOnce sync.Once
)

func bwrapPath() string {
	if runtime.GOOS != "linux" {
		return ""
	}
	bwrapOnce.Do(func() {
		p, err := exec.LookPath("bwrap")
		if err == nil {
			bwrap = p
		}
	})
	return bwrap
}

// CanSandbox returns whether or not
// tenants can be sandboxed using bwrap(1).
func CanSandbox() bool {
	return bwrapPath() != ""
}

func (m *Manager) sandboxStart(cmd *exec.Cmd, cg cgroup.Dir, cachedir string) error {
	bw := bwrapPath()
	// pipe for --block-fd
	piper, pipew, err := os.Pipe()
	if err != nil {
		return err
	}
	// mount / as read-only,
	// and bind-mount CACHEDIR over /tmp
	//
	// TODO: maybe don't bind all of /
	// and instead make a template for a
	// minimal rootfs visible to the tenant process?
	args := []string{
		bw,
		"--unshare-pid",
		"--ro-bind", "/", "/",
		"--proc", "/proc",
		"--dev", "/dev",
		"--bind", cachedir, "/tmp",
		"--ro-bind", "/var/empty", "/var", // don't make /var visible
		"--die-with-parent",
		// override CACHEDIR to /tmp, since
		// we have bind-mounted the original cache directory
		// to a new location
		"--setenv", "CACHEDIR", "/tmp",
		"--block-fd", strconv.Itoa(len(cmd.ExtraFiles) + 3),
		"--",
	}
	cmd.ExtraFiles = append(cmd.ExtraFiles, piper)
	args = append(args, cmd.Args...)
	cmd.Path = bw
	cmd.Args = append(args, cmd.Args...)
	err = cmd.Start()
	if err != nil {
		piper.Close()
		pipew.Close()
		return err
	}
	piper.Close()
	// move the child into the new target cgroup
	if !cg.IsZero() {
		err = cgroup.Move(cmd.Process.Pid, cg)
		if err != nil {
			m.errorf("moving child into cgroup: %s", err)
		}
	}
	// produce signal for child to start
	// now that we have changed its cgroup:
	pipew.WriteString("\n")
	pipew.Close()
	return nil
}
