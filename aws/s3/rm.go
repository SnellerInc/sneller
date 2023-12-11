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
