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
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/SnellerInc/sneller/fsutil"
)

var (
	_ fsutil.WalkGlobFS = &BucketFS{}
)

// split a glob pattern on the first meta-character
// so that we can list from the most specific prefix
func splitMeta(pattern string) (string, string) {
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*', '?', '\\', '[':
			return pattern[:i], pattern[i:]
		default:
		}
	}
	return pattern, ""
}

func (b *BucketFS) client() *http.Client {
	if b.Client != nil {
		return b.Client
	}
	return &DefaultClient
}

// WalkGlob implements fsutil.WalkGlobFS
func (p *Prefix) WalkGlob(seek, pattern string, walk fsutil.WalkGlobFn) error {
	if seek != "" && seek != "." {
		seek = path.Join(p.Path, seek)
	}
	return (&BucketFS{
		Key:    p.Key,
		Bucket: p.Bucket,
		Client: p.Client,
	}).WalkGlob(seek, path.Join(p.Path, pattern), walk)
}

// WalkGlob implements fsutil.WalkGlobFS
//
// S3 globbing is accelerated by listing
// all the objects that begin with the
// leading non-meta-character characters
// of pattern, followed by filtering each
// of the listed objects by pattern.
func (b *BucketFS) WalkGlob(seek, pattern string, walk fsutil.WalkGlobFn) error {
	if !ValidBucket(b.Bucket) {
		return badBucket(b.Bucket)
	}
	// check pattern is sane
	if _, err := path.Match(pattern, ""); err != nil {
		return err
	}
	before, after := splitMeta(pattern)
	if after == "" {
		// no meta-characters; we are
		// just opening a file
		rd, err := b.sub(before).readDirAt(1)
		if err == io.EOF || (len(rd) == 0 && err == nil) {
			return nil
		}
		if err != nil {
			return err
		}
		if f, ok := rd[0].(*File); ok {
			return walk(f.Path(), f, nil)
		}
		return nil
	}
	// path.Clean will have normalized "" into ".",
	// but S3 doesn't know or care about "."
	if seek == "." {
		seek = ""
	}
	// the start parameter is only meaningful
	// if it is "larger" than the prefix being listed;
	// otherwise we should reject it
	// (AWS S3 accepts redundant start-after params,
	// but Minio rejects them)
	//
	// see equivalent check in fsutil.WalkGlob
	if seek != "" && (seek < before || !strings.HasPrefix(seek, before)) {
		return fmt.Errorf("seek %q not compatible with prefix %q", seek, before)
	}
	return b.globAt(seek, before, pattern, walk)
}

func isDir(p string) bool {
	return p[len(p)-1] == '/'
}

func (b *BucketFS) globAt(start, pre, pattern string, walk fsutil.WalkGlobFn) error {
	cont := ""
	for {
		parts := []string{
			"list-type=2",
			"max-keys=1000",
			"prefix=" + queryEscape(pre),
		}
		if cont != "" {
			parts = append(parts, "continuation-token="+url.QueryEscape(cont))
		}
		if start != "" && start > pre {
			parts = append(parts, "start-after="+queryEscape(start))
		}
		sort.Strings(parts)
		query := "?" + strings.Join(parts, "&")
		req, err := http.NewRequest(http.MethodGet, rawURI(b.Key, b.Bucket, query), nil)
		if err != nil {
			return fmt.Errorf("creating http request: %w", err)
		}
		b.Key.SignV4(req, nil)
		res, err := b.client().Do(req)
		if err != nil {
			return fmt.Errorf("executing request: %w", err)
		}
		if res.StatusCode != 200 {
			res.Body.Close()
			return fmt.Errorf("s3 list objects: %s", res.Status)
		}
		ret := struct {
			IsTruncated bool `xml:"IsTruncated"`
			Contents    []struct {
				ETag         string    `xml:"ETag"`
				Name         string    `xml:"Key"`
				LastModified time.Time `xml:"LastModified"`
				Size         int64     `sml:"Size"`
			} `xml:"Contents"`
			NextToken string `xml:"NextContinuationToken"`
		}{}
		err = xml.NewDecoder(res.Body).Decode(&ret)
		res.Body.Close()
		if err != nil {
			return fmt.Errorf("xml decoding response: %w", err)
		}
		for i := range ret.Contents {
			match, err := path.Match(pattern, ret.Contents[i].Name)
			if err != nil {
				return err
			}
			// in the AWS console, creating a "folder"
			// writes a 0-byte object ending in '/',
			// which we'd like to filter out
			if !match || (ret.Contents[i].Size == 0 && isDir(ret.Contents[i].Name)) {
				continue
			}
			f := &File{
				Reader: &Reader{
					Key:          b.Key,
					Client:       b.client(),
					ETag:         ret.Contents[i].ETag,
					LastModified: ret.Contents[i].LastModified,
					size:         ret.Contents[i].Size,
					bucket:       b.Bucket,
					object:       ret.Contents[i].Name,
				},
			}
			err = walk(ret.Contents[i].Name, f, nil)
			if err != nil {
				return err
			}
		}
		if !ret.IsTruncated {
			break
		}
		cont = ret.NextToken
	}
	return nil
}
