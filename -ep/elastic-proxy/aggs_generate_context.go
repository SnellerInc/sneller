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

const DummyAlias = "$dummy$"

type aggsGenerateContext struct {
	context             *QueryContext
	parent              *aggsGenerateContext
	bucket              string
	currentAggregations map[string]aggregation
	query               expression
	size                int
	groupExprs          []projectAliasExpr
	groupKeyIndex       int
	projections         []projectAliasExpr
	orderBy             []orderByExpr
	nestingLevel        int
}

func (c *aggsGenerateContext) clone() *aggsGenerateContext {
	return &aggsGenerateContext{
		context:             c.context,
		bucket:              c.bucket,
		currentAggregations: c.currentAggregations,
		query:               c.query,
		size:                c.size,
		groupExprs:          append([]projectAliasExpr{}, c.groupExprs...),
		orderBy:             c.orderBy,
		nestingLevel:        c.nestingLevel,
	}
}

func (c *aggsGenerateContext) setBucket(subBucket string) *aggsGenerateContext {
	if c.bucket != "" {
		c.bucket = c.bucket + ":" + subBucket
	} else {
		c.bucket = subBucket
	}

	return c
}

func (c *aggsGenerateContext) addOrdering(ordering ...orderByExpr) *aggsGenerateContext {
	c.orderBy = append(c.orderBy, ordering...)
	return c
}

func (c *aggsGenerateContext) andQuery(q expression) *aggsGenerateContext {
	if q != nil {
		if c.query == nil {
			c.query = q
		} else {
			c.query = &exprOperator2{
				Context:  c.context,
				Operator: "AND",
				Expr1:    c.query,
				Expr2:    q,
			}
		}
	}
	return c
}

func (c *aggsGenerateContext) addGroupExpr(e expression) *aggsGenerateContext {
	c.groupExprs = append(c.groupExprs, projectAliasExpr{
		Context:    c.context,
		Alias:      fmt.Sprintf("%s:%s%%%d", KeyPrefix, c.bucket, c.groupKeyIndex),
		expression: e,
	})
	c.groupKeyIndex++
	return c
}

func (c *aggsGenerateContext) setSize(size *int) *aggsGenerateContext {
	c.size = effectiveSize(size)
	return c
}

func (c *aggsGenerateContext) makeCountStar() *exprFunction {
	return &exprFunction{
		Context: c.context,
		Name:    "COUNT",
		Exprs:   []expression{&exprFieldName{Context: c.context}},
	}
}

func (c *aggsGenerateContext) addDocCount(sort bool) *aggsGenerateContext {
	countStarExpr := c.makeCountStar()
	c = c.addProjection(DocCount, countStarExpr)
	if sort {
		c = c.addOrdering(orderByExpr{
			Context:    c.context,
			expression: countStarExpr,
			Order:      "DESC",
		})
	}
	return c
}

func (c *aggsGenerateContext) addProjection(alias string, e expression) *aggsGenerateContext {
	c.projections = append(c.projections, projectAliasExpr{
		Context:    c.context,
		Alias:      alias,
		expression: e,
	})
	return c
}

func (c *aggsGenerateContext) allGroupExprs() []projectAliasExpr {
	if c.parent == nil {
		return c.groupExprs
	}
	return append(c.parent.allGroupExprs(), c.groupExprs...)
}

func (c *aggsGenerateContext) transform() ([]projectAliasExpr, error) {
	var subQueries []projectAliasExpr
	for _, subBucket := range sortedKeys(c.currentAggregations) {
		aggregation := c.currentAggregations[subBucket]

		// Metric aggregations
		if ba, ok := aggregation.Aggregation.(metricAggregation); ok {
			err := ba.transform(subBucket, c)
			if err != nil {
				return nil, err
			}
		}

		// Bucket aggregations
		if ba, ok := aggregation.Aggregation.(bucketAggregation); ok {
			subContext := &aggsGenerateContext{
				context:             c.context,
				parent:              c,
				nestingLevel:        c.nestingLevel + 1,
				currentAggregations: aggregation.SubAggregations,
			}
			if c.bucket != "" {
				subContext.bucket = fmt.Sprintf("%s:%s", c.bucket, subBucket)
			} else {
				subContext.bucket = subBucket
			}
			filtersQueries, err := ba.transform(subContext)
			if err != nil {
				return nil, err
			}
			if filtersQueries != nil {
				subQueries = append(subQueries, filtersQueries...)
			}
		}
	}

	allGroupExprs := c.allGroupExprs()

	groupByExpr := make([]expression, len(allGroupExprs))
	for i, bpe := range allGroupExprs {
		groupByExpr[i] = bpe.expression
	}

	where := c.query
	if c.nestingLevel > 1 {
		parentGroups := c.parent.allGroupExprs()

		// generate SELECT of the parent nodes
		const SelectionSource = "$selection"
		parentBucket := fmt.Sprintf("%s:%s%%%d", BucketPrefix, c.parent.bucket, 0)
		parentSelect := exprSelect{
			Context: c.context,
			From:    []expression{ParseExprSourceNameWithAlias(c.context, parentBucket, SelectionSource)},
		}
		for _, pg := range parentGroups {
			parentSelect.Projection = append(parentSelect.Projection, projectAliasExpr{
				Context: c.context,
				expression: &exprFieldName{
					Context: c.context,
					Source:  SelectionSource,
					Fields:  []string{pg.Alias},
				},
			})
		}

		var sourceExpr expression
		if len(parentGroups) > 1 {
			// Use the { '$key1': .., '$key2': .. } IN PARENT format
			sourceFields := make([]exprObjectField, len(parentGroups))
			selectFields := make([]exprObjectField, len(parentGroups))
			for i, pg := range parentGroups {
				sourceFields[i] = exprObjectField{
					Name: pg.Alias,
					Expr: pg.expression,
				}
				selectFields[i] = exprObjectField{
					Name: pg.Alias,
					Expr: &exprFieldName{
						Context: c.context,
						Source:  SelectionSource,
						Fields:  []string{pg.Alias},
					},
				}
			}
			sourceExpr = &exprObject{
				Context: c.context,
				Fields:  sourceFields,
			}
			parentSelect.Projection = []projectAliasExpr{
				{
					Context: c.context,
					expression: &exprObject{
						Context: c.context,
						Fields:  selectFields,
					},
				},
			}
		} else {
			// Use the "$key1" IN PARENT format
			parentSelect.Projection = []projectAliasExpr{
				{
					Context: c.context,
					expression: &exprFieldName{
						Context: c.context,
						Source:  SelectionSource,
						Fields:  []string{parentGroups[0].Alias},
					},
				},
			}
			sourceExpr = parentGroups[0].expression
		}

		inExpr := exprOperator2{
			Context:  c.context,
			Operator: "IN",
			Expr1:    sourceExpr,
			Expr2:    &parentSelect,
		}

		if where == nil {
			where = &inExpr
		} else {
			where = &exprOperator2{
				Context:  c.context,
				Operator: "AND",
				Expr1:    &inExpr,
				Expr2:    where,
			}
		}
	}

	var queries []projectAliasExpr

	if len(c.projections) > 0 {
		mainSelect := exprSelect{
			Context:    c.context,
			Projection: append(allGroupExprs, c.projections...),
			From:       c.context.Sources,
			Where:      where,
			GroupBy:    groupByExpr,
		}

		if c.size > 0 {
			if c.nestingLevel > 1 {
				parentGroups := c.parent.allGroupExprs()
				partitionBy := make([]expression, len(parentGroups))
				for i, pg := range parentGroups {
					partitionBy[i] = pg.expression
				}
				mainSelect.Having = &exprOperator2{
					Operator: "<=",
					Expr1: &exprOperatorOver{
						Context: c.context,
						Function: exprFunction{
							Context: c.context,
							Name:    "ROW_NUMBER",
						},
						PartitionBy: partitionBy,
						OrderBy:     c.orderBy,
					},
					Expr2: &exprJSONLiteral{
						Context: c.context,
						Value:   JSONLiteral{Value: c.size},
					},
				}

			} else {
				mainSelect.Limit = c.size
			}
		}

		// Workaround for https://github.com/SnellerInc/sneller-core/issues/1214
		if len(mainSelect.Projection) == 1 {
			mainSelect.Projection = append(mainSelect.Projection, projectAliasExpr{
				Context:    c.context,
				Alias:      DummyAlias,
				expression: &exprJSONLiteral{Value: JSONLiteral{false}},
			})
		}

		// use specified sort order (if set)
		if len(c.orderBy) > 0 {
			for _, orderBy := range c.orderBy {
				// check if the expression is also projected using an alias
				for _, proj := range mainSelect.Projection {
					if proj.expression == orderBy.expression {
						orderBy = orderByExpr{
							Context:    c.context,
							expression: ParseExprSourceName(c.context, proj.Alias),
							Order:      orderBy.Order,
						}
						break
					}
				}
				mainSelect.OrderBy = append(mainSelect.OrderBy, orderBy)
			}
		}

		queries = []projectAliasExpr{
			{
				Context:    c.context,
				Alias:      fmt.Sprintf("%s:%s%%%d", BucketPrefix, c.bucket, 0),
				expression: &mainSelect,
			},
		}
	}

	if len(subQueries) > 0 {
		queries = append(queries, subQueries...)
	}

	return queries, nil
}
