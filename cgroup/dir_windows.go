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

//go:build !linux

// Package cgroup implements a thin wrapper
// around the Linux cgroupv2 filesystem API.
// For more information, please consult the
// relevant kernel documentation.
package cgroup

// Dir is an absolute directory path
// (including the mount path of the cgroup2 mountpoint).
type Dir string

// IsZero returns true if d is the zero value of Dir.
// (The zero value of Dir is not a valid cgroup directory.)
func (d Dir) IsZero() bool { return d == "" }

// Move moves an existing process into
// the cgroup specified by into.
func Move(pid int, into Dir) error {
	panic("unimplemented")
}

// Kill kills all the processes in a cgroup.
// However, it does not remove the cgroup directory.
func (d Dir) Kill() error {
	panic("unimplemented")
}

// Remove removes the cgroup. Only empty cgroups
// may be removed, so the caller may need to call
// Kill first in order to ensure the cgroup is empty.
func (d Dir) Remove() error {
	panic("unimplemented")
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
	panic("unimplemented")
}

// Sub returns a new Dir that represents a
// sub-directory of d.
func (d Dir) Sub(dir string) Dir {
	panic("unimplemented")
}
