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

// Package cgroup implements a thin wrapper
// around the Linux cgroupv2 filesystem API.
// For more information, please consult the
// relevant kernel documentation.
package cgroup

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// Dir is an absolute directory path
// (including the mount path of the cgroup2 mountpoint).
type Dir string

// IsZero returns true if d is the zero value of Dir.
// (The zero value of Dir is not a valid cgroup directory.)
func (d Dir) IsZero() bool { return d == "" }

// Root returns the first found cgroup2
// mountpoint from /proc/mounts.
func Root() (Dir, error) {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return "", err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		parts := strings.Fields(s.Text())
		if len(parts) >= 3 &&
			parts[2] == "cgroup2" {
			return Dir(parts[1]), nil
		}
	}
	if s.Err() != nil {
		return "", err
	}
	return "", fs.ErrNotExist
}

// Sub returns a new Dir that represents a
// sub-directory of d.
func (d Dir) Sub(dir string) Dir { return Dir(d.join(dir)) }

// Self returns the cgroup of the current process,
// provided that the current process is *only* a member
// of a cgroup2 and not a legacy cgroup1 hierarchy.
func Self() (Dir, error) {
	text, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", err
	}
	if len(text) < 3 || text[0] != '0' || text[1] != ':' || text[2] != ':' {
		return "", fmt.Errorf("don't understand /proc/self/cgroup (are you using systemd?): %s", text)
	}
	text = bytes.TrimSpace(text)
	i := bytes.IndexByte(text, '/')
	if i < 0 {
		return "", fmt.Errorf("%s is not a valid cgroup", text)
	}
	root, err := Root()
	if err != nil {
		return "", err
	}
	return root.Sub(string(text[i:])), nil
}

// WriteInt writes the provided integer value
// plus a newline character to the file
// with the given name within d.
func (d Dir) WriteInt(name string, val int) error {
	buf := strconv.AppendInt(nil, int64(val), 10)
	return d.WriteLine(name, buf)
}

func (d Dir) join(name string) string { return filepath.Join(string(d), name) }

// WriteLine writes the provided bytes plus
// a newline character to the file with the
// given name within d.
func (d Dir) WriteLine(name string, buf []byte) error {
	f, err := os.OpenFile(d.join(name), os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(buf, '\n'))
	return err
}

// IsDelegated returns (true, nil) if a
// process with the given uid+gid can add
// processes to d, or (false, nil) otherwise.
// IsDelegated will report an error if the
// cgroup doesn't exist.
func (d Dir) IsDelegated(uid, gid int) (bool, error) {
	fi, err := os.Stat(d.join("cgroup.procs"))
	if err != nil {
		return false, err
	}
	if uid == 0 {
		return true, nil
	}
	sys, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return false, fmt.Errorf("unexpected fs.FileInfo.Sys: %T", fi.Sys())
	}
	perm := fi.Mode().Perm()
	if perm&2 != 0 {
		// write-other
		return true, nil
	}
	// write-gid
	if sys.Gid == uint32(gid) && (perm>>3)&2 != 0 {
		return true, nil
	}
	// write-owner
	if sys.Uid == uint32(uid) && (perm>>6)&2 != 0 {
		return true, nil
	}
	return false, nil
}

// Move moves an existing process into
// the cgroup specified by into.
func Move(pid int, into Dir) error {
	return into.WriteInt("cgroup.procs", pid)
}

// Kill kills all the processes in a cgroup.
// However, it does not remove the cgroup directory.
func (d Dir) Kill() error {
	return d.WriteInt("cgroup.kill", 1)
}

// Remove removes the cgroup. Only empty cgroups
// may be removed, so the caller may need to call
// Kill first in order to ensure the cgroup is empty.
func (d Dir) Remove() error {
	return os.Remove(string(d))
}

// Procs returns the list of pids that
// currently occupy a cgroup.
// (This corresponds to the cgroup.procs file within the cgroup directory.)
func (d Dir) Procs() ([]int, error) {
	f, err := os.Open(d.join("cgroup.procs"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var lst []int
	s := bufio.NewScanner(f)
	for s.Scan() {
		i, err := strconv.Atoi(s.Text())
		if err != nil {
			return lst, err
		}
		lst = append(lst, i)
	}
	return lst, s.Err()
}

// Create creates a new directory sub under
// the existing group d. If the directory
// doesn't already exist, it is created.
// If the directory *does* exist, then the
// behavior of Create depends on the 'kill'
// flag: if kill is set to true, then all
// the sub-processes in the existing cgroup
// are killed. If kill is set to false, then
// an error is returned (matching fs.ErrExist)
// and the cgroup is left unmodified.
func (d Dir) Create(sub string, kill bool) (Dir, error) {
	p := d.join(sub)
	err := os.Mkdir(p, 0755)
	if err != nil {
		if errors.Is(err, fs.ErrExist) && kill {
			pd := d.Sub(sub)
			return pd, pd.Kill()
		}
		return "", err
	}
	return Dir(p), nil
}
