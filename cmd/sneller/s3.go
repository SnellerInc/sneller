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
	"fmt"
	"os"
	"strings"

	"github.com/SnellerInc/sneller/aws"
	"github.com/SnellerInc/sneller/aws/s3"
	"github.com/SnellerInc/sneller/vm"
)

func getKey(service, bucket string) *aws.SigningKey {
	sk, err := aws.AmbientKey(service, s3.DeriveForBucket(bucket))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return sk
}

func s3object(name string) (vm.Table, error) {
	name = strings.TrimPrefix(name, "s3://")
	splitidx := strings.IndexByte(name, '/')
	bucket := name[:splitidx]
	object := name[splitidx+1:]
	key := getKey("s3", bucket)
	obj, err := s3.Stat(key, bucket, object)
	if err != nil {
		return nil, err
	}
	return srcTable(obj, obj.Size(), nil)
}

// s3-backed NDJSON table
func s3nd(name string) (vm.Table, error) {
	name = strings.TrimPrefix(name, "s3://")
	splitidx := strings.IndexByte(name, '/')
	bucket := name[:splitidx]
	object := name[splitidx+1:]
	key := getKey("s3", bucket)
	obj, err := s3.Stat(key, bucket, object)
	if err != nil {
		return nil, err
	}
	return &jstable{in: obj, size: obj.Size()}, nil
}
