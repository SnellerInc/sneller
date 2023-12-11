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

package regexp2

import (
	"unicode/utf8"
)

// escapeChar is the rune used as the escape character.
const escapeChar = rune(0x5C) // backslash

// nodeIDT type of nodes in NFA/DFA
type nodeIDT int32

// stateIDT type of states in DFA data-structures
type stateIDT int32

// groupIDT type of observation groups
type groupIDT int

const edgeEpsilonRune = rune(utf8.MaxRune)
const edgeAnyRune = rune(utf8.MaxRune + 1)
const edgeAnyNotLfRune = rune(utf8.MaxRune + 2)
const edgeRLZARune = rune(utf8.MaxRune + 3)
const edgeLfRune = rune('\n')
