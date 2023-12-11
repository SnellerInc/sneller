// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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
