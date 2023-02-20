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

import "fmt"

type aggsFilter struct {
	Query
}

func (f *aggsFilter) transform(c *aggsGenerateContext) ([]projectAliasExpr, error) {
	fExpr, err := f.Query.Expression(c.context)
	if err != nil {
		if err != nil {
			return nil, fmt.Errorf("cannot parse query in 'filter' aggregation %q: %w", c.bucket, err)
		}
	}

	subContext := c.andQuery(fExpr).addDocCount(true)
	return subContext.transform()
}

func (f *aggsFilter) process(c *aggsProcessContext) (any, error) {
	group, _ := c.data.(*groupResults)

	bucketResult, err := c.subResult(group)
	if err != nil {
		return nil, err
	}

	var docCount int64
	if group != nil {
		docCount, err = group.docCount()
		if err != nil {
			return nil, err
		}
	}

	return &bucketSingleResult{
		SubAggregations: bucketResult,
		DocCount:        docCount,
	}, nil
}
