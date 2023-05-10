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

package db

import (
	"fmt"
	"strings"

	"github.com/SnellerInc/sneller/ion/blockfmt"
)

// NewLocalTenant creates a tenant that uses given FS as backend
func NewLocalTenant(fs InputFS) Tenant {
	return &localTenant{fs: fs}
}

// NewLocalTenantFromPath creates a tenats that uses DirFS with given path
func NewLocalTenantFromPath(path string) Tenant {
	return NewLocalTenant(NewDirFS(path))
}

// localTenant implements Tenant backed by DirFS
type localTenant struct {
	fs InputFS
}

func (t *localTenant) ID() string {
	return "local"
}

var localTenantKey blockfmt.Key = blockfmt.Key{
	0x52, 0xfd, 0xfc, 0x07, 0x21, 0x82, 0x65, 0x4f,
	0x16, 0x3f, 0x5f, 0x0f, 0x9a, 0x62, 0x1d, 0x72,
	0x95, 0x66, 0xc7, 0x4d, 0x10, 0x03, 0x7c, 0x4d,
	0x7b, 0xbb, 0x04, 0x07, 0xd1, 0xe2, 0xc6, 0x49,
}

func (t *localTenant) Key() *blockfmt.Key {
	return &localTenantKey
}

func (t *localTenant) Root() (InputFS, error) {
	return t.fs, nil
}

func (t *localTenant) Split(pattern string) (InputFS, string, error) {
	const prefix = "file://"
	newpat := strings.TrimPrefix(pattern, prefix)
	if len(newpat) == len(pattern) {
		return nil, "", fmt.Errorf("pattern %q has to start with %q", pattern, prefix)
	}

	return t.fs, newpat, nil
}
