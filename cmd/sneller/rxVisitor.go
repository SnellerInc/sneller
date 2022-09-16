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
	"io"

	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/regexp2"
	"golang.org/x/sys/cpu"
)

// rxVisitor is a regex visitor struct; it visits all nodes in an expression, and it will retrieve
// the data-structure and dot graph of the _first_ regex it encounters
type rxVisitor struct {
	dst           io.Writer
	ds, dot, done bool // once the first regex is found, we are done
	err           error
}

func (r *rxVisitor) Visit(node expr.Node) expr.Visitor {
	if r.done {
		return nil
	}
	n, ok := node.(*expr.Comparison)
	if !ok {
		return r
	}
	if (n.Op != expr.SimilarTo) && (n.Op != expr.RegexpMatch) && (n.Op != expr.RegexpMatchCi) {
		return r
	}

	regexStrOrg := string(n.Right.(expr.String))
	regexType := regexp2.SimilarTo
	if n.Op == expr.RegexpMatch {
		regexType = regexp2.Regexp
	} else if n.Op == expr.RegexpMatchCi {
		regexType = regexp2.RegexpCi
	}
	if regex, err := regexp2.Compile(regexStrOrg, regexType); err != nil {
		// ignore strings that are not valid regexes
		r.err = err
	} else if err := regexp2.IsSupported(regexStrOrg); err != nil {
		// ignore not supported regexes
		r.err = err
	} else {
		if store, err := regexp2.CompileDFA(regex, regexp2.MaxNodesAutomaton); err != nil {
			r.err = err
		} else {
			exprEscaped := regexp2.PrettyStrForDot(regex.String())
			infoStr := fmt.Sprintf("\nregex=%v  (original)\nregex=%v  (effective)\n", regexStrOrg, exprEscaped)
			hasUnicodeEdge := store.HasUnicodeEdge()

			if cpu.X86.HasAVX512VBMI && !hasUnicodeEdge {
				hasWildcard, wildcardRange := store.HasUnicodeWildcard()
				if dsTiny, err := regexp2.NewDsTiny(store); err == nil {
					nNodes := store.NumberOfNodes()
					nGroups := dsTiny.NumberOfGroups()
					if ds, valid, dot := dsTiny.DataWithGraphviz(true, 6, hasWildcard, wildcardRange); valid {
						if r.ds {
							_, r.err = fmt.Fprintf(r.dst, "Tiny6: %v\n", infoStr)
							regexp2.DumpDebug(r.dst, ds, 6, nNodes, nGroups, exprEscaped)
						}
						if r.dot {
							r.err = dot.DotContent(r.dst, "Tiny6", exprEscaped)
						}
						r.done = true
						return nil
					}
					if ds, valid, dot := dsTiny.DataWithGraphviz(true, 7, hasWildcard, wildcardRange); valid {
						if r.ds {
							_, r.err = fmt.Fprintf(r.dst, "Tiny7: %v\n", infoStr)
							regexp2.DumpDebug(r.dst, ds, 7, nNodes, nGroups, exprEscaped)
						}
						if r.dot {
							r.err = dot.DotContent(r.dst, "Tiny7", exprEscaped)
						}
						r.done = true
						return nil
					}
					if ds, valid, dot := dsTiny.DataWithGraphviz(true, 8, hasWildcard, wildcardRange); valid {
						if r.ds {
							_, r.err = fmt.Fprintf(r.dst, "Tiny8: %v\n", infoStr)
							regexp2.DumpDebug(r.dst, ds, 8, nNodes, nGroups, exprEscaped)
						}
						if r.dot {
							r.err = dot.DotContent(r.dst, "Tiny8", exprEscaped)
						}
						r.done = true
						return nil
					}
				}
			}
			if dsLarge, err := regexp2.NewDsLarge(store); err == nil {
				if r.ds {
					if _, err = fmt.Fprintf(r.dst, "%v\n", infoStr); err != nil {
						r.err = err
					} else {
						r.err = dsLarge.DumpDebug(r.dst)
					}
				}
				if r.dot {
					store.MergeEdgeRanges(false)
					r.err = store.Dot().DotContent(r.dst, "Large", exprEscaped)
				}
				r.done = true
				return nil
			}
		}
	}
	r.done = true
	return nil
}

// GraphvizDFA dumps the DFA of the first regex
// to 'dst' as dot(1)-compatible text.
func GraphvizDFA(q *expr.Query, dst io.Writer, ds, dot bool) error {
	r := &rxVisitor{dst, ds, dot, false, nil}
	expr.Walk(r, q.Body)
	return r.err
}
