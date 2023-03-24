package partiql

// Code generated automatically; DO NOT EDIT

import (
	"github.com/SnellerInc/sneller/expr"
)

func lookupKeyword(word []byte) (int, int) {
	n := len(word)
	if n < 2 || n > 21 {
		return -1, -1
	}
	switch n {
	case 2:
		switch asciiUpper(word[1]) {
		case 'N':
			if asciiUpper(word[0]) == 'O' {
				return ON, -1
			}
			if asciiUpper(word[0]) == 'I' {
				return IN, -1
			}
		case 'O':
			if asciiUpper(word[0]) == 'T' {
				return TO, -1
			}
		case 'R':
			if asciiUpper(word[0]) == 'O' {
				return OR, -1
			}
		case 'S':
			if asciiUpper(word[0]) == 'A' {
				return AS, -1
			}
			if asciiUpper(word[0]) == 'I' {
				return IS, -1
			}
		case 'T':
			if asciiUpper(word[0]) == 'A' {
				return AT, -1
			}
		case 'Y':
			if asciiUpper(word[0]) == 'B' {
				return BY, -1
			}
		}
	case 3:
		switch asciiUpper(word[1]) {
		case 'A':
			if asciiUpper(word[0]) == 'M' && asciiUpper(word[2]) == 'X' {
				return AGGREGATE, int(expr.OpMax)
			}
		case 'I':
			if asciiUpper(word[0]) == 'M' && asciiUpper(word[2]) == 'N' {
				return AGGREGATE, int(expr.OpMin)
			}
		case 'L':
			if asciiUpper(word[0]) == 'A' && asciiUpper(word[2]) == 'L' {
				return ALL, -1
			}
		case 'N':
			if asciiUpper(word[0]) == 'A' && asciiUpper(word[2]) == 'D' {
				return AND, -1
			}
			if asciiUpper(word[0]) == 'E' && asciiUpper(word[2]) == 'D' {
				return END, -1
			}
		case 'O':
			if asciiUpper(word[0]) == 'N' && asciiUpper(word[2]) == 'T' {
				return NOT, -1
			}
		case 'S':
			if asciiUpper(word[0]) == 'A' && asciiUpper(word[2]) == 'C' {
				return ASC, -1
			}
		case 'U':
			if asciiUpper(word[0]) == 'S' && asciiUpper(word[2]) == 'M' {
				return AGGREGATE, int(expr.OpSum)
			}
		case 'V':
			if asciiUpper(word[0]) == 'A' && asciiUpper(word[2]) == 'G' {
				return AGGREGATE, int(expr.OpAvg)
			}
		}
	case 4:
		switch asciiUpper(word[0]) {
		case 'B':
			if equalASCIILetters4([4]byte(word), [4]byte{'B', 'O', 'T', 'H'}) {
				return BOTH, -1
			}
		case 'C':
			if equalASCIILetters4([4]byte(word), [4]byte{'C', 'A', 'S', 'T'}) {
				return CAST, -1
			}
			if equalASCIILetters4([4]byte(word), [4]byte{'C', 'A', 'S', 'E'}) {
				return CASE, -1
			}
		case 'D':
			if equalASCIILetters4([4]byte(word), [4]byte{'D', 'E', 'S', 'C'}) {
				return DESC, -1
			}
		case 'E':
			if equalASCIILetters4([4]byte(word), [4]byte{'E', 'L', 'S', 'E'}) {
				return ELSE, -1
			}
		case 'F':
			if equalASCIILetters4([4]byte(word), [4]byte{'F', 'R', 'O', 'M'}) {
				return FROM, -1
			}
		case 'I':
			if equalASCIILetters4([4]byte(word), [4]byte{'I', 'N', 'T', 'O'}) {
				return INTO, -1
			}
		case 'J':
			if equalASCIILetters4([4]byte(word), [4]byte{'J', 'O', 'I', 'N'}) {
				return JOIN, -1
			}
		case 'L':
			if equalASCIILetters4([4]byte(word), [4]byte{'L', 'I', 'K', 'E'}) {
				return LIKE, -1
			}
			if equalASCIILetters4([4]byte(word), [4]byte{'L', 'E', 'F', 'T'}) {
				return LEFT, -1
			}
			if equalASCIILetters4([4]byte(word), [4]byte{'L', 'A', 'S', 'T'}) {
				return LAST, -1
			}
		case 'N':
			if equalASCIILetters4([4]byte(word), [4]byte{'N', 'U', 'L', 'L'}) {
				return NULL, -1
			}
		case 'O':
			if equalASCIILetters4([4]byte(word), [4]byte{'O', 'V', 'E', 'R'}) {
				return OVER, -1
			}
		case 'R':
			if equalASCIILetters4([4]byte(word), [4]byte{'R', 'A', 'N', 'K'}) {
				return AGGREGATE, int(expr.OpRank)
			}
		case 'T':
			if equalASCIILetters4([4]byte(word), [4]byte{'T', 'R', 'U', 'E'}) {
				return TRUE, -1
			}
			if equalASCIILetters4([4]byte(word), [4]byte{'T', 'H', 'E', 'N'}) {
				return THEN, -1
			}
			if equalASCIILetters4([4]byte(word), [4]byte{'T', 'R', 'I', 'M'}) {
				return TRIM, -1
			}
		case 'W':
			if equalASCIILetters4([4]byte(word), [4]byte{'W', 'H', 'E', 'N'}) {
				return WHEN, -1
			}
			if equalASCIILetters4([4]byte(word), [4]byte{'W', 'I', 'T', 'H'}) {
				return WITH, -1
			}
		}
	case 5:
		switch asciiUpper(word[0]) {
		case 'C':
			if equalASCIILetters5([5]byte(word), [5]byte{'C', 'R', 'O', 'S', 'S'}) {
				return CROSS, -1
			}
			if equalASCIILetters5([5]byte(word), [5]byte{'C', 'O', 'U', 'N', 'T'}) {
				return AGGREGATE, int(expr.OpCount)
			}
		case 'E':
			if equalASCIILetters5([5]byte(word), [5]byte{'E', 'V', 'E', 'R', 'Y'}) {
				return AGGREGATE, int(expr.OpBoolAnd)
			}
		case 'F':
			if equalASCIILetters5([5]byte(word), [5]byte{'F', 'A', 'L', 'S', 'E'}) {
				return FALSE, -1
			}
			if equalASCIILetters5([5]byte(word), [5]byte{'F', 'I', 'R', 'S', 'T'}) {
				return FIRST, -1
			}
		case 'G':
			if equalASCIILetters5([5]byte(word), [5]byte{'G', 'R', 'O', 'U', 'P'}) {
				return GROUP, -1
			}
		case 'I':
			if equalASCIILetters5([5]byte(word), [5]byte{'I', 'L', 'I', 'K', 'E'}) {
				return ILIKE, -1
			}
			if equalASCIILetters5([5]byte(word), [5]byte{'I', 'N', 'N', 'E', 'R'}) {
				return INNER, -1
			}
		case 'L':
			if equalASCIILetters5([5]byte(word), [5]byte{'L', 'I', 'M', 'I', 'T'}) {
				return LIMIT, -1
			}
		case 'N':
			if equalASCIILetters5([5]byte(word), [5]byte{'N', 'U', 'L', 'L', 'S'}) {
				return NULLS, -1
			}
		case 'O':
			if equalASCIILetters5([5]byte(word), [5]byte{'O', 'R', 'D', 'E', 'R'}) {
				return ORDER, -1
			}
		case 'R':
			if equalASCIILetters5([5]byte(word), [5]byte{'R', 'I', 'G', 'H', 'T'}) {
				return RIGHT, -1
			}
		case 'U':
			if equalASCIILetters5([5]byte(word), [5]byte{'U', 'N', 'I', 'O', 'N'}) {
				return UNION, -1
			}
		case 'V':
			if equalASCIILetters5([5]byte(word), [5]byte{'V', 'A', 'L', 'U', 'E'}) {
				return VALUE, -1
			}
		case 'W':
			if equalASCIILetters5([5]byte(word), [5]byte{'W', 'H', 'E', 'R', 'E'}) {
				return WHERE, -1
			}
		}
	case 6:
		switch asciiUpper(word[0]) {
		case 'B':
			if equalASCII(word, []byte("BIT_OR")) {
				return AGGREGATE, int(expr.OpBitOr)
			}
		case 'C':
			if equalASCIILetters6([6]byte(word), [6]byte{'C', 'O', 'N', 'C', 'A', 'T'}) {
				return CONCAT, -1
			}
		case 'E':
			if equalASCIILetters6([6]byte(word), [6]byte{'E', 'X', 'I', 'S', 'T', 'S'}) {
				return EXISTS, -1
			}
			if equalASCIILetters6([6]byte(word), [6]byte{'E', 'S', 'C', 'A', 'P', 'E'}) {
				return ESCAPE, -1
			}
		case 'F':
			if equalASCIILetters6([6]byte(word), [6]byte{'F', 'I', 'L', 'T', 'E', 'R'}) {
				return FILTER, -1
			}
		case 'H':
			if equalASCIILetters6([6]byte(word), [6]byte{'H', 'A', 'V', 'I', 'N', 'G'}) {
				return HAVING, -1
			}
		case 'L':
			if equalASCIILetters6([6]byte(word), [6]byte{'L', 'A', 'T', 'E', 'S', 'T'}) {
				return AGGREGATE, int(expr.OpLatest)
			}
		case 'N':
			if equalASCIILetters6([6]byte(word), [6]byte{'N', 'U', 'L', 'L', 'I', 'F'}) {
				return NULLIF, -1
			}
		case 'O':
			if equalASCIILetters6([6]byte(word), [6]byte{'O', 'F', 'F', 'S', 'E', 'T'}) {
				return OFFSET, -1
			}
		case 'S':
			if equalASCIILetters6([6]byte(word), [6]byte{'S', 'E', 'L', 'E', 'C', 'T'}) {
				return SELECT, -1
			}
			if equalASCIILetters6([6]byte(word), [6]byte{'S', 'T', 'D', 'D', 'E', 'V'}) {
				return AGGREGATE, int(expr.OpStdDevPop)
			}
		case 'U':
			if equalASCIILetters6([6]byte(word), [6]byte{'U', 'T', 'C', 'N', 'O', 'W'}) {
				return UTCNOW, -1
			}
		}
	case 7:
		switch asciiUpper(word[3]) {
		case 'D':
			if equalASCIILetters7([7]byte(word), [7]byte{'L', 'E', 'A', 'D', 'I', 'N', 'G'}) {
				return LEADING, -1
			}
		case 'I':
			if equalASCIILetters7([7]byte(word), [7]byte{'S', 'I', 'M', 'I', 'L', 'A', 'R'}) {
				return SIMILAR, -1
			}
			if equalASCIILetters7([7]byte(word), [7]byte{'U', 'N', 'P', 'I', 'V', 'O', 'T'}) {
				return UNPIVOT, -1
			}
		case 'L':
			if equalASCIILetters7([7]byte(word), [7]byte{'E', 'X', 'P', 'L', 'A', 'I', 'N'}) {
				return EXPLAIN, -1
			}
			if equalASCII(word, []byte("BOOL_OR")) {
				return AGGREGATE, int(expr.OpBoolOr)
			}
		case 'R':
			if equalASCIILetters7([7]byte(word), [7]byte{'E', 'X', 'T', 'R', 'A', 'C', 'T'}) {
				return EXTRACT, -1
			}
		case 'S':
			if equalASCIILetters7([7]byte(word), [7]byte{'M', 'I', 'S', 'S', 'I', 'N', 'G'}) {
				return MISSING, -1
			}
		case 'W':
			if equalASCIILetters7([7]byte(word), [7]byte{'B', 'E', 'T', 'W', 'E', 'E', 'N'}) {
				return BETWEEN, -1
			}
		case '_':
			if equalASCII(word, []byte("BIT_AND")) {
				return AGGREGATE, int(expr.OpBitAnd)
			}
			if equalASCII(word, []byte("BIT_XOR")) {
				return AGGREGATE, int(expr.OpBitXor)
			}
		}
	case 8:
		switch asciiUpper(word[0]) {
		case 'B':
			if equalASCII(word, []byte("BOOL_AND")) {
				return AGGREGATE, int(expr.OpBoolAnd)
			}
		case 'C':
			if equalASCIILetters8([8]byte(word), [8]byte{'C', 'O', 'A', 'L', 'E', 'S', 'C', 'E'}) {
				return COALESCE, -1
			}
		case 'D':
			if equalASCII(word, []byte("DATE_ADD")) {
				return DATE_ADD, -1
			}
			if equalASCIILetters8([8]byte(word), [8]byte{'D', 'I', 'S', 'T', 'I', 'N', 'C', 'T'}) {
				return DISTINCT, -1
			}
		case 'E':
			if equalASCIILetters8([8]byte(word), [8]byte{'E', 'A', 'R', 'L', 'I', 'E', 'S', 'T'}) {
				return AGGREGATE, int(expr.OpEarliest)
			}
		case 'T':
			if equalASCIILetters8([8]byte(word), [8]byte{'T', 'R', 'A', 'I', 'L', 'I', 'N', 'G'}) {
				return TRAILING, -1
			}
		case 'V':
			if equalASCIILetters8([8]byte(word), [8]byte{'V', 'A', 'R', 'I', 'A', 'N', 'C', 'E'}) {
				return AGGREGATE, int(expr.OpVariancePop)
			}
		}
	case 9:
		if equalASCII(word, []byte("DATE_DIFF")) {
			return DATE_DIFF, -1
		}
		if equalASCIILetters9([9]byte(word), [9]byte{'P', 'A', 'R', 'T', 'I', 'T', 'I', 'O', 'N'}) {
			return PARTITION, -1
		}
	case 10:
		switch asciiUpper(word[1]) {
		case 'A':
			if equalASCII(word, []byte("DATE_TRUNC")) {
				return DATE_TRUNC, -1
			}
		case 'E':
			if equalASCII(word, []byte("DENSE_RANK")) {
				return AGGREGATE, int(expr.OpDenseRank)
			}
		case 'O':
			if equalASCII(word, []byte("ROW_NUMBER")) {
				return AGGREGATE, int(expr.OpRowNumber)
			}
		case 'T':
			if equalASCII(word, []byte("STDDEV_POP")) {
				return AGGREGATE, int(expr.OpStdDevPop)
			}
		}
	case 12:
		if equalASCII(word, []byte("VARIANCE_POP")) {
			return AGGREGATE, int(expr.OpVariancePop)
		}
	case 17:
		if equalASCII(word, []byte("SNELLER_DATASHAPE")) {
			return AGGREGATE, int(expr.OpSystemDatashape)
		}
	case 21:
		if equalASCII(word, []byte("APPROX_COUNT_DISTINCT")) {
			return AGGREGATE, int(expr.OpApproxCountDistinct)
		}
	}
	return -1, -1
}

func equalASCIILetters4(anyCase [4]byte, upperCaseLetters [4]byte) bool {
	for i := range upperCaseLetters {
		if (upperCaseLetters[i]^anyCase[i])&0xdf != 0 {
			return false
		}
	}
	return true
}

func equalASCIILetters5(anyCase [5]byte, upperCaseLetters [5]byte) bool {
	for i := range upperCaseLetters {
		if (upperCaseLetters[i]^anyCase[i])&0xdf != 0 {
			return false
		}
	}
	return true
}

func equalASCIILetters6(anyCase [6]byte, upperCaseLetters [6]byte) bool {
	for i := range upperCaseLetters {
		if (upperCaseLetters[i]^anyCase[i])&0xdf != 0 {
			return false
		}
	}
	return true
}

func equalASCIILetters7(anyCase [7]byte, upperCaseLetters [7]byte) bool {
	for i := range upperCaseLetters {
		if (upperCaseLetters[i]^anyCase[i])&0xdf != 0 {
			return false
		}
	}
	return true
}

func equalASCIILetters8(anyCase [8]byte, upperCaseLetters [8]byte) bool {
	for i := range upperCaseLetters {
		if (upperCaseLetters[i]^anyCase[i])&0xdf != 0 {
			return false
		}
	}
	return true
}

func equalASCIILetters9(anyCase [9]byte, upperCaseLetters [9]byte) bool {
	for i := range upperCaseLetters {
		if (upperCaseLetters[i]^anyCase[i])&0xdf != 0 {
			return false
		}
	}
	return true
}

// checksum: 7590873419eae19568dda65d569a9ada
