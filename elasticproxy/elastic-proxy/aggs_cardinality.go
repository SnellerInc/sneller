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

type aggsCardinality struct {
	fieldMetricAgg
}

func (f *aggsCardinality) transform(subBucket string, c *aggsGenerateContext) error {
	c.addProjection(subBucket, &exprFunction{
		Context: c.context,
		Name:    "COUNT",
		Exprs:   []expression{&exprOperator1{Context: c.context, Operator: "DISTINCT", Expr1: ParseExprFieldName(c.context, f.Field)}},
	})
	return nil
}

func (f *aggsCardinality) process(c *aggsProcessContext) (any, error) {
	v, _ := c.result()
	if v == nil {
		v = 0 // use 0 as the default value
	}
	return &metricResult{v}, nil
}
