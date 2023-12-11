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

package elastic_proxy

import (
	"github.com/amazon-ion/ion-go/ion"
)

func ConvertION(v any) any {
	switch vv := v.(type) {
	case []any:
		for i, v := range vv {
			vv[i] = ConvertION(v)
		}
		return vv
	case map[string]any:
		for k, v := range vv {
			vv[k] = ConvertION(v)
		}
		return vv
	case *ion.Timestamp:
		return vv.GetDateTime()
	default:
		return vv
	}
}
