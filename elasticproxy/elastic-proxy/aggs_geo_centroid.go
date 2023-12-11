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

type aggsGeoCentroid struct {
	fieldMetricAgg
}

func (f *aggsGeoCentroid) transform(subBucket string, c *aggsGenerateContext) error {
	exprs := exprObject{
		Context: c.context,
		Fields: []exprObjectField{
			{Name: "lat", Expr: &exprFunction{Context: c.context, Name: "AVG", Exprs: []expression{ParseExprFieldName(c.context, f.Field+LatExt)}}},
			{Name: "lon", Expr: &exprFunction{Context: c.context, Name: "AVG", Exprs: []expression{ParseExprFieldName(c.context, f.Field+LonExt)}}},
		},
	}
	c.addProjection(subBucket, &exprs)
	return nil
}

func (f *aggsGeoCentroid) process(c *aggsProcessContext) (any, error) {
	v, _ := c.result()
	return &locationResult{v}, nil
}

type locationResult struct {
	Location any `json:"location"`
}
