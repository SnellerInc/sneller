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

package regexp2

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
	"unicode/utf8"
)

// MaxNodesAutomaton is the maximum number of states when constructing and transforming NFAs and DFAs.
const MaxNodesAutomaton = 3000

// MaxCharInRegex is the maximum number of characters in a regex string
const MaxCharInRegex = 1000

// IsSupported determines whether expr is a supported regex; return nil if supported, error otherwise
func IsSupported(expr string) error {
	nRunesExpr := utf8.RuneCountInString(expr)
	if nRunesExpr > MaxCharInRegex {
		return fmt.Errorf("provided regex expression contains %v code-points which is more than the max %v", nRunesExpr, MaxCharInRegex)
	}
	return nil
}

type RegexType int

const (
	SimilarTo RegexType = iota
	Regexp
	RegexpCi
	GolangSimilarTo
	GolangRegexp
)

// Compile return a regex for the provided string and regexType.
func Compile(expr string, regexType RegexType) (regex *regexp.Regexp, err error) {
	exprOrg := expr

	if regexType == SimilarTo || regexType == GolangSimilarTo {
		exprRunes := []rune(expr)
		newRegexRunes := make([]rune, 0, len(exprRunes))
		for index, r := range exprRunes {
			escaped := (index > 0) && (exprRunes[index-1] == escapeChar)
			switch r {
			case '.', '^', '$': // characters '.', '^' and '$' are NOT meta-characters in "SIMILAR TO",
				if escaped {
					// found an escaped char, do not escape it again
					newRegexRunes = append(newRegexRunes, r)
				} else {
					newRegexRunes = append(newRegexRunes, escapeChar, r)
				}
			case '%': // replace '%': it represents n character
				if escaped {
					// found an escaped char, do not change it
					newRegexRunes = append(newRegexRunes, r)
				} else {
					newRegexRunes = append(newRegexRunes, '.', '*')
				}
			case '_': // replace '_': it represents a single character
				if escaped {
					// found an escaped char, do not change it
					newRegexRunes = append(newRegexRunes, r)
				} else {
					newRegexRunes = append(newRegexRunes, '.')
				}
			default:
				newRegexRunes = append(newRegexRunes, r)
			}
		}
		expr = string(newRegexRunes)
	}

	switch regexType {
	case SimilarTo:
		if !strings.HasSuffix(exprOrg, "$") {
			expr = "(" + expr + ")$" // NOTE brackets are necessary
		}
	case GolangSimilarTo:
		if !strings.HasPrefix(exprOrg, "^") {
			expr = "^(" + expr + ")" // NOTE brackets are necessary
		}
		if !strings.HasSuffix(exprOrg, "$") {
			expr = "(" + expr + ")$" // NOTE brackets are necessary
		}
	case RegexpCi:
		if !strings.HasPrefix(exprOrg, "(?i)") {
			expr = "(?i)" + expr
		}
	case Regexp:
		if !strings.HasPrefix(exprOrg, "^") {
			expr = "(.|\n)*(" + expr + ")" // NOTE brackets are necessary
		}
	case GolangRegexp:
		// do nothing
	}
	return regexp.Compile(expr)
}

// extractProg extracts the internal syntax.Prog from regexp.Regexp instance using reflection
func extractProg(regex *regexp.Regexp) *syntax.Prog {
	return (*syntax.Prog)(reflect.ValueOf(regex).Elem().FieldByName("prog").UnsafePointer())
}

// extractNFA extracts the NFA from regexp.Regexp instance using Go
func extractNFA(regex *regexp.Regexp, maxNodes int) (*NFAStore, error) {
	// extract the NFA data-structure that has been created by Go to handle the provided regex
	p := extractProg(regex)
	// create an empty store of nodes
	store := newNFAStore(maxNodes)

	// create translation map for nodeIDs from golangNFA to our NFA
	translation := newMap[int, nodeIDT]()
	{
		idSet := newSet[int]()
		for from := range p.Inst {
			i := &p.Inst[from]
			idSet.insert(from)

			switch i.Op {
			case syntax.InstAlt, syntax.InstAltMatch:
				idSet.insert(int(i.Out))
				idSet.insert(int(i.Arg))
			case syntax.InstCapture, syntax.InstEmptyWidth, syntax.InstRune, syntax.InstNop, syntax.InstRune1, syntax.InstRuneAny, syntax.InstRuneAnyNotNL:
				idSet.insert(int(i.Out))
			case syntax.InstMatch, syntax.InstFail:
				// do nothing
			}
		}
		for id := range idSet {
			nodeID, err := store.newNode()
			if err != nil {
				return nil, fmt.Errorf("%v::extractNFA", err)
			}
			translation.insert(id, nodeID)
		}
	}

	// initialize the start node
	store.startIDi = translation.at(p.Start)
	if startNode, err := store.get(store.startIDi); err != nil {
		return nil, err
	} else {
		startNode.start = true
	}

	// iterate over all instruction in the golangNFA and add nodes and edges to our NFA
	for from := range p.Inst {
		node, err := store.get(translation.at(from))
		if err != nil {
			return nil, err
		}
		i := &p.Inst[from]

		switch i.Op {
		case syntax.InstAlt:
			node.addEdgeRune(edgeEpsilonRune, translation.at(int(i.Out)), false)
			node.addEdgeRune(edgeEpsilonRune, translation.at(int(i.Arg)), false)
		case syntax.InstAltMatch:
			//NOTE dead code: InstAltMatch is nowhere issued, but when it will (in some future)...
			node.addEdgeRune(edgeEpsilonRune, translation.at(int(i.Out)), false)
			node.addEdgeRune(edgeEpsilonRune, translation.at(int(i.Arg)), false)
		case syntax.InstCapture:
			node.addEdgeRune(edgeEpsilonRune, translation.at(int(i.Out)), false)
		case syntax.InstEmptyWidth:
			nodeTo := translation.at(int(i.Out))
			switch syntax.EmptyOp(i.Arg) {
			//NOTE EmptyEndLine is issued for POSIX regex; EmptyEndText is issued for NON-POSIX regex
			case syntax.EmptyEndText:
				//NOTE non-posix $: $ matches then end-of-line AND end-of-text
				node.addEdgeInternal(edgeT{edgeRLZARange, nodeTo})
			case syntax.EmptyEndLine:
				//NOTE posix $: $ matches the end-of-line
				node.addEdgeInternal(edgeT{newSymbolRange(edgeEpsilonRune, edgeEpsilonRune), nodeTo})
			case syntax.EmptyBeginLine, syntax.EmptyBeginText, syntax.EmptyNoWordBoundary, syntax.EmptyWordBoundary:
				node.addEdgeInternal(edgeT{newSymbolRange(edgeEpsilonRune, edgeEpsilonRune), nodeTo})
			default:
				node.addEdgeRune(edgeEpsilonRune, nodeTo, false)
			}
		case syntax.InstNop:
			node.addEdgeRune(edgeEpsilonRune, translation.at(int(i.Out)), false)
		case syntax.InstMatch: // no i.Out
			node.accept = true
		case syntax.InstFail: // no i.Out
		case syntax.InstRune: // i.Rune is a sequence of rune ranges
			caseSensitive := (syntax.Flags(i.Arg) & syntax.FoldCase) == 0
			nRunes := len(i.Rune)
			if nRunes == 1 {
				node.addEdgeRune(i.Rune[0], translation.at(int(i.Out)), caseSensitive)
			} else {
				if (nRunes & 1) == 1 {
					return nil, fmt.Errorf("received invalid sequence of rune ranges from GOLANG: %#U", i.Rune)
				}
				seq := i.Rune
				for nRunes > 0 {
					node.addEdge(newSymbolRange(seq[0], seq[1]), translation.at(int(i.Out)))
					nRunes -= 2
					seq = seq[2:]
				}
			}
		case syntax.InstRune1:
			caseSensitive := (syntax.Flags(i.Arg) & syntax.FoldCase) == 0
			for _, r := range i.Rune {
				node.addEdgeRune(r, translation.at(int(i.Out)), caseSensitive)
			}
		case syntax.InstRuneAny:
			node.addEdgeRune(edgeAnyRune, translation.at(int(i.Out)), false)
		case syntax.InstRuneAnyNotNL:
			node.addEdgeRune(edgeAnyNotLfRune, translation.at(int(i.Out)), false)
		}
	}
	return &store, nil
}

func CompileDFA(regex *regexp.Regexp, maxNodes int) (*DFAStore, error) {
	return CompileDFADebug(regex, false, maxNodes)
}

func CompileDFADebug(regex *regexp.Regexp, writeDot bool, maxNodes int) (*DFAStore, error) {
	tmpPath := os.TempDir() + "\\sneller\\"
	if writeDot {
		os.MkdirAll(tmpPath, os.ModeDir)
		log.Printf("6af9e7a9 going to write dot files to your temp dir %v for regex %q", tmpPath, regex.String())
	}
	name := "sneller"

	nfaStore, err := extractNFA(regex, maxNodes)
	if err != nil {
		return nil, fmt.Errorf("%v::CompileDFA", err)
	}
	if writeDot {
		name += "_nfa"
		nfaStore.dot().WriteToFile(tmpPath+name+".dot", name, regex.String())
	}

	if err = nfaStore.pruneRLZ(); err != nil {
		return nil, fmt.Errorf("%v::CompileDFA", err)
	}
	if writeDot {
		name += "_prn"
		nfaStore.dot().WriteToFile(tmpPath+name+".dot", name, regex.String())
	}

	if err = nfaStore.refactorEdges(); err != nil {
		return nil, fmt.Errorf("%v::CompileDFA", err)
	}
	if writeDot {
		name += "_ref"
		nfaStore.dot().WriteToFile(tmpPath+name+".dot", name, regex.String())
	}
	dfaStore, err := nfaToDfa(nfaStore, maxNodes)
	if err != nil {
		return nil, fmt.Errorf("%v::CompileDFA", err)
	}
	if writeDot {
		name += "_dfa"
		dfaStore.Dot().WriteToFile(tmpPath+name+".dot", name, regex.String())
	}

	if err = dfaStore.pruneUnreachable(); err != nil {
		return nil, fmt.Errorf("%v::CompileDFA", err)
	}
	if err = dfaStore.pruneNeverAccepting(); err != nil {
		return nil, fmt.Errorf("%v::CompileDFA", err)
	}

	if writeDot {
		name += "_prn"
		dfaStore.Dot().WriteToFile(tmpPath+name+".dot", name, regex.String())
	}

	dfaStore, err = minDfa(dfaStore, maxNodes)
	if err != nil {
		return nil, fmt.Errorf("%v::CompileDFA", err)
	}

	dfaStore.removeEdgesFromAcceptNodes() // remove all outgoing edges from accepting nodes

	// we can merge accept nodes since they do not have outgoing edges (anymore)
	if err := dfaStore.mergeAcceptNodes(); err != nil {
		return nil, fmt.Errorf("%v::CompileDFA", err)
	}
	if writeDot {
		name += "_min"
		dfaStore.Dot().WriteToFile(tmpPath+name+".dot", name, regex.String())
	}
	return dfaStore, nil
}

// PrettyStrForDot makes the string pretty and usable for printing in dot files
func PrettyStrForDot(str string) string {
	str = strconv.Quote(strings.ReplaceAll(str, "\n", "0xA"))
	return str[1 : len(str)-1] // trim leading and trailing '"'
}
