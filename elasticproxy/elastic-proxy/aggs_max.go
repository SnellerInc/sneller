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

type aggsMax struct {
	fieldMetricAgg
}

func (f *aggsMax) transform(subBucket string, c *aggsGenerateContext) error {
	c.addProjection(subBucket, &exprFunction{
		Context: c.context,
		Name:    "MAX",
		Exprs:   []expression{ParseExprFieldName(c.context, f.Field)},
	})
	return nil
}

func (f *aggsMax) process(c *aggsProcessContext) (any, error) {
	v, _ := c.result()
	return &metricResult{v}, nil
}
