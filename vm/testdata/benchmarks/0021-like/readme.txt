Different tests test different bycodes
					OLD 					NEW
LIKE "a" 	 		equalconst				equalconst
LIKE "a%" 			contains_prefix_cs		contains_prefix_cs
LIKE "%a" 			contains_suffix_cs		contains_suffix_cs
LIKE "%a%" 			match_pat_cs			contains_substr_cs
LIKE "%a_b%			match_pat_cs			contains_pattern_cs
LIKE "%a%b%"		2x match_pat_cs 		2x contains_substr_cs
LIKE "%a_b%c_d%		2x match_pat_cs			2x contains_pattern_cs

a string is handled as an UTF8 patters iff it contains a non-ASCII value, OR ASCII 's' and 'k'
					OLD						NEW
ILIKE "a" 			cmp_str_eq_ci			cmp_str_eq_ci
ILIKE "a%" 			contains_prefix_ci		contains_prefix_ci
ILIKE "%a" 			contains_suffix_ci		contains_suffix_ci
ILIKE "%a%" 		match_pat_ci			contains_substr_ci
ILIKE "%a_b%		match_pat_ci			contains_pattern_ci
ILIKE "%a%b%" 		2x match_pat_ci     	2x contains_substr_ci
ILIKE "%a_b%c_d%	2x match_pat_ci			2x contains_pattern_ci

					OLD						NEW
ILIKE "ø" 			cmp_str_eq_utf8_ci		cmp_str_eq_utf8_ci
ILIKE "ø%" 			contains_prefix_utf8_ci contains_prefix_utf8_ci
ILIKE "%ø"			contains_suffix_utf8_ci	contains_suffix_utf8_ci
ILIKE "%ø%" 		match_pat_utf8_ci		contains_substr_utf8_ci
ILIKE "%ø_a%" 		match_pat_utf8_ci    	contains_pattern_utf8_ci
ILIKE "%ø%ø%" 		2x match_pat_utf8_ci	2x contains_substr_utf8_ci
ILIKE "%ø_a%ø_a%"	2x match_pat_utf8_ci	2x contains_pattern_utf8_ci
