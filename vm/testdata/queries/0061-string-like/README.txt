Different tests test different opcodes

LIKE with needle type "a" -> opCmpStrEqCs
LIKE with needle type "a_b" -> opEqPatternCs
LIKE with needle type "%a" -> opContainsSuffixCs
LIKE with needle type "a%" -> opContainsPrefixCs
LIKE with needle type "%a%" -> opContainsSubstrCs
LIKE with needle type "%a_b%" -> opContainsPatternCs

a string is handled as an Unicode patterns iff it contains a non-ASCII value, OR ASCII 's' and 'k'

ILIKE with ASCII needle type "a" -> opCmpStrEqCi
ILIKE with ASCII needle type "a_b" -> opEqPatternCi
ILIKE with ASCII needle type "%a" -> opContainsSuffixCi
ILIKE with ASCII needle type "a%" -> opContainsPrefixCi
ILIKE with ASCII needle type "%a%" -> opContainsSubstrCi
ILIKE with ASCII needle type "%a_b%" -> opContainsPatternCi

ILIKE with Unicode needle type "a" -> opCmpStrEqUTF8Ci
ILIKE with Unicode needle type "a_b" -> opEqPatternUTF8Ci
ILIKE with Unicode needle type "%a" -> opContainsSuffixUTF8Ci
ILIKE with Unicode needle type "a%" -> opContainsPrefixUTF8Ci
ILIKE with Unicode needle type "%a%" -> opContainsSubstrUTF8Ci
ILIKE with Unicode needle type "%a_b%" -> opContainsPatternUTF8Ci

