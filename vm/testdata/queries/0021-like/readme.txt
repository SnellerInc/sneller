Different tests test different bycodes

LIKE with needle type "a" -> opCmpStrEqCs
LIKE with needle type "%a" -> opContainsSuffixCs
LIKE with needle type "a%" -> opContainsPrefixCs
LIKE with needle type "%a%" -> opContainsSubstrCs
LIKE with needle type "%a%b%" etc -> opMatchpatCs

a string is handled as an UTF8 patters iff it contains a non-ASCII value, OR ASCII 's' and 'k'

ILIKE with ASCII needle type "a" -> opCmpStrEqCi
ILIKE with ASCII needle type "%a" -> opContainsSuffixCi
ILIKE with ASCII needle type "a%" -> opContainsPrefixCi
ILIKE with ASCII needle type "%a%" -> opContainsSubstrCi
ILIKE with ASCII needle type "%a%b%" etc -> opMatchpatCi

ILIKE with UTF8 needle type "a" -> opCmpStrEqUTF8Ci
ILIKE with UTF8 needle type "%a" -> opContainsSuffixUTF8Ci
ILIKE with UTF8 needle type "a%" -> opContainsPrefixUTF8Ci
ILIKE with UTF8 needle type "%a%" -> opContainsSubstrUTF8Ci
ILIKE with UTF8 needle type "%a%b%" etc -> opMatchpatUTF8Ci
