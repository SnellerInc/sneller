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

package s3

import (
	"fmt"
	"io/fs"
	"net/http"
	"path"
)

// Remove removes the object at fullpath.
func (b *BucketFS) Remove(fullpath string) error {
	fullpath = path.Clean(fullpath)
	if !fs.ValidPath(fullpath) {
		return fmt.Errorf("%s: %s", fullpath, fs.ErrInvalid)
	}
	req, err := http.NewRequestWithContext(b.Ctx, http.MethodDelete, uri(b.Key, b.Bucket, fullpath), nil)
	if err != nil {
		return err
	}
	b.Key.SignV4(req, nil)
	client := b.Client
	if client == nil {
		client = &DefaultClient
	}
	res, err := flakyDo(client, req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 204 {
		return fmt.Errorf("s3 DELETE: %s %s", res.Status, extractMessage(res.Body))
	}
	return nil
}
