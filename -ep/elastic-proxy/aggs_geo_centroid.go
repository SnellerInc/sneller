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
