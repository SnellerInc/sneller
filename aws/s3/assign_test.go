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

package s3_test

import (
	"github.com/SnellerInc/sneller/aws/s3"
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/fsutil"
)

// we can't do this inside the s3 package
// due to circular imports, but we can do it here:
var _ fsutil.OpenRangeFS = &s3.BucketFS{}
var _ fsutil.VisitDirFS = &s3.BucketFS{}
var _ fsutil.VisitDirFS = &s3.Prefix{}
var _ fsutil.NamedFile = &s3.File{}
var _ db.ContextFS = &s3.BucketFS{}
var _ db.ContextFS = &s3.Prefix{}
