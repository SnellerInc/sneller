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

package main

import (
	"fmt"
)

func evalString(s string) (int, error) {
	tokens, err := lexExpression(s)
	if err != nil {
		return 0, err
	}

	return evalExpression(tokens)
}

func evalExpression(x any) (int, error) {
	if arr, ok := x.([]any); ok {
		return evalMathExpression(arr)
	}

	return evalSingleExpression(x)
}

// evalMathExpression evaluates expressions having
// numbers, symbols, '+' and '*'
func evalMathExpression(tokens []any) (int, error) {
	if len(tokens) == 1 {
		return evalSingleExpression(tokens[0])
	}

	// replace all symbols with their values and evaluate multiplications
	var terms []int
	mult := false
	push := func(val int) {
		if mult {
			terms[len(terms)-1] *= val
			mult = false
		} else {
			terms = append(terms, val)
		}
	}

	for _, t := range tokens {
		switch v := t.(type) {
		case int:
			push(v)
		case uint8:
			mult = (v == '*')
			if v != '*' && v != '+' {
				return 0, fmt.Errorf("unexpected operator %c", v)
			}
		case string:
			num, err := evalSingleExpression(t)
			if err != nil {
				return 0, err
			}
			push(num)
		}
	}

	num := 0
	for _, x := range terms {
		num += x
	}

	return num, nil
}

func evalSingleExpression(token any) (int, error) {
	switch v := token.(type) {
	case int:
		return v, nil

	case string:
		switch v {
		case "BC_DICT_SIZE":
			return bcDictSize, nil
		case "BC_SLOT_SIZE":
			return bcSlotSize, nil
		case "BC_AGGSLOT_SIZE":
			return bcAggSlotSize, nil
		case "BC_LITREF_SIZE":
			return bcLitRefSize, nil
		case "BC_IMM16_SIZE":
			return bcImm16Size, nil
		case "BC_IMM64_SIZE":
			return bcImm64Size, nil
		}
	}

	return 0, fmt.Errorf("cannot evaluate %v", token)
}
