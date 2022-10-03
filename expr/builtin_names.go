package expr

// Code generated automatically; DO NOT EDIT

var builtin2Name = [109]string{
	"CONCAT",                   // Concat
	"TRIM",                     // Trim
	"LTRIM",                    // Ltrim
	"RTRIM",                    // Rtrim
	"UPPER",                    // Upper
	"LOWER",                    // Lower
	"CONTAINS",                 // Contains
	"CONTAINS_CI",              // ContainsCI
	"EQUALS_CI",                // EqualsCI
	"CHAR_LENGTH",              // CharLength
	"IS_SUBNET_OF",             // IsSubnetOf
	"SUBSTRING",                // Substring
	"SPLIT_PART",               // SplitPart
	"BIT_COUNT",                // BitCount
	"ABS",                      // Abs
	"SIGN",                     // Sign
	"ROUND",                    // Round
	"ROUND_EVEN",               // RoundEven
	"TRUNC",                    // Trunc
	"FLOOR",                    // Floor
	"CEIL",                     // Ceil
	"SQRT",                     // Sqrt
	"CBRT",                     // Cbrt
	"EXP",                      // Exp
	"EXPM1",                    // ExpM1
	"EXP2",                     // Exp2
	"EXP10",                    // Exp10
	"HYPOT",                    // Hypot
	"LN",                       // Ln
	"LN1P",                     // Ln1p
	"LOG",                      // Log
	"LOG2",                     // Log2
	"LOG10",                    // Log10
	"POW",                      // Pow
	"PI",                       // Pi
	"DEGREES",                  // Degrees
	"RADIANS",                  // Radians
	"SIN",                      // Sin
	"COS",                      // Cos
	"TAN",                      // Tan
	"ASIN",                     // Asin
	"ACOS",                     // Acos
	"ATAN",                     // Atan
	"ATAN2",                    // Atan2
	"LEAST",                    // Least
	"GREATEST",                 // Greatest
	"WIDTH_BUCKET",             // WidthBucket
	"DATE_ADD_MICROSECOND",     // DateAddMicrosecond
	"DATE_ADD_MILLISECOND",     // DateAddMillisecond
	"DATE_ADD_SECOND",          // DateAddSecond
	"DATE_ADD_MINUTE",          // DateAddMinute
	"DATE_ADD_HOUR",            // DateAddHour
	"DATE_ADD_DAY",             // DateAddDay
	"DATE_ADD_WEEK",            // DateAddWeek
	"DATE_ADD_MONTH",           // DateAddMonth
	"DATE_ADD_QUARTER",         // DateAddQuarter
	"DATE_ADD_YEAR",            // DateAddYear
	"DATE_DIFF_MICROSECOND",    // DateDiffMicrosecond
	"DATE_DIFF_MILLISECOND",    // DateDiffMillisecond
	"DATE_DIFF_SECOND",         // DateDiffSecond
	"DATE_DIFF_MINUTE",         // DateDiffMinute
	"DATE_DIFF_HOUR",           // DateDiffHour
	"DATE_DIFF_DAY",            // DateDiffDay
	"DATE_DIFF_WEEK",           // DateDiffWeek
	"DATE_DIFF_MONTH",          // DateDiffMonth
	"DATE_DIFF_QUARTER",        // DateDiffQuarter
	"DATE_DIFF_YEAR",           // DateDiffYear
	"DATE_EXTRACT_MICROSECOND", // DateExtractMicrosecond
	"DATE_EXTRACT_MILLISECOND", // DateExtractMillisecond
	"DATE_EXTRACT_SECOND",      // DateExtractSecond
	"DATE_EXTRACT_MINUTE",      // DateExtractMinute
	"DATE_EXTRACT_HOUR",        // DateExtractHour
	"DATE_EXTRACT_DAY",         // DateExtractDay
	"DATE_EXTRACT_DOW",         // DateExtractDOW
	"DATE_EXTRACT_DOY",         // DateExtractDOY
	"DATE_EXTRACT_MONTH",       // DateExtractMonth
	"DATE_EXTRACT_QUARTER",     // DateExtractQuarter
	"DATE_EXTRACT_YEAR",        // DateExtractYear
	"DATE_TRUNC_MICROSECOND",   // DateTruncMicrosecond
	"DATE_TRUNC_MILLISECOND",   // DateTruncMillisecond
	"DATE_TRUNC_SECOND",        // DateTruncSecond
	"DATE_TRUNC_MINUTE",        // DateTruncMinute
	"DATE_TRUNC_HOUR",          // DateTruncHour
	"DATE_TRUNC_DAY",           // DateTruncDay
	"DATE_TRUNC_DOW",           // DateTruncDOW
	"DATE_TRUNC_MONTH",         // DateTruncMonth
	"DATE_TRUNC_QUARTER",       // DateTruncQuarter
	"DATE_TRUNC_YEAR",          // DateTruncYear
	"TO_UNIX_EPOCH",            // ToUnixEpoch
	"TO_UNIX_MICRO",            // ToUnixMicro
	"GEO_HASH",                 // GeoHash
	"GEO_TILE_X",               // GeoTileX
	"GEO_TILE_Y",               // GeoTileY
	"GEO_TILE_ES",              // GeoTileES
	"GEO_DISTANCE",             // GeoDistance
	"SIZE",                     // ObjectSize
	"TABLE_GLOB",               // TableGlob
	"TABLE_PATTERN",            // TablePattern
	"IN_SUBQUERY",              // InSubquery
	"HASH_LOOKUP",              // HashLookup
	"IN_REPLACEMENT",           // InReplacement
	"HASH_REPLACEMENT",         // HashReplacement
	"SCALAR_REPLACEMENT",       // ScalarReplacement
	"STRUCT_REPLACEMENT",       // StructReplacement
	"LIST_REPLACEMENT",         // ListReplacement
	"TIME_BUCKET",              // TimeBucket
	"MAKE_LIST",                // MakeList
	"MAKE_STRUCT",              // MakeStruct
	"TYPE_BIT",                 // TypeBit
}

func name2Builtin(s string) BuiltinOp {
	switch s {
	case "CONCAT":
		return Concat
	case "TRIM":
		return Trim
	case "LTRIM":
		return Ltrim
	case "RTRIM":
		return Rtrim
	case "UPPER":
		return Upper
	case "LOWER":
		return Lower
	case "CONTAINS":
		return Contains
	case "CONTAINS_CI":
		return ContainsCI
	case "EQUALS_CI":
		return EqualsCI
	case "CHAR_LENGTH":
		return CharLength
	case "CHARACTER_LENGTH":
		return CharLength
	case "IS_SUBNET_OF":
		return IsSubnetOf
	case "SUBSTRING":
		return Substring
	case "SPLIT_PART":
		return SplitPart
	case "BIT_COUNT":
		return BitCount
	case "ABS":
		return Abs
	case "SIGN":
		return Sign
	case "ROUND":
		return Round
	case "ROUND_EVEN":
		return RoundEven
	case "TRUNC":
		return Trunc
	case "FLOOR":
		return Floor
	case "CEIL":
		return Ceil
	case "CEILING":
		return Ceil
	case "SQRT":
		return Sqrt
	case "CBRT":
		return Cbrt
	case "EXP":
		return Exp
	case "EXPM1":
		return ExpM1
	case "EXP2":
		return Exp2
	case "EXP10":
		return Exp10
	case "HYPOT":
		return Hypot
	case "LN":
		return Ln
	case "LN1P":
		return Ln1p
	case "LOG":
		return Log
	case "LOG2":
		return Log2
	case "LOG10":
		return Log10
	case "POW":
		return Pow
	case "POWER":
		return Pow
	case "PI":
		return Pi
	case "DEGREES":
		return Degrees
	case "RADIANS":
		return Radians
	case "SIN":
		return Sin
	case "COS":
		return Cos
	case "TAN":
		return Tan
	case "ASIN":
		return Asin
	case "ACOS":
		return Acos
	case "ATAN":
		return Atan
	case "ATAN2":
		return Atan2
	case "LEAST":
		return Least
	case "GREATEST":
		return Greatest
	case "WIDTH_BUCKET":
		return WidthBucket
	case "DATE_ADD_MICROSECOND":
		return DateAddMicrosecond
	case "DATE_ADD_MILLISECOND":
		return DateAddMillisecond
	case "DATE_ADD_SECOND":
		return DateAddSecond
	case "DATE_ADD_MINUTE":
		return DateAddMinute
	case "DATE_ADD_HOUR":
		return DateAddHour
	case "DATE_ADD_DAY":
		return DateAddDay
	case "DATE_ADD_WEEK":
		return DateAddWeek
	case "DATE_ADD_MONTH":
		return DateAddMonth
	case "DATE_ADD_QUARTER":
		return DateAddQuarter
	case "DATE_ADD_YEAR":
		return DateAddYear
	case "DATE_DIFF_MICROSECOND":
		return DateDiffMicrosecond
	case "DATE_DIFF_MILLISECOND":
		return DateDiffMillisecond
	case "DATE_DIFF_SECOND":
		return DateDiffSecond
	case "DATE_DIFF_MINUTE":
		return DateDiffMinute
	case "DATE_DIFF_HOUR":
		return DateDiffHour
	case "DATE_DIFF_DAY":
		return DateDiffDay
	case "DATE_DIFF_WEEK":
		return DateDiffWeek
	case "DATE_DIFF_MONTH":
		return DateDiffMonth
	case "DATE_DIFF_QUARTER":
		return DateDiffQuarter
	case "DATE_DIFF_YEAR":
		return DateDiffYear
	case "DATE_EXTRACT_MICROSECOND":
		return DateExtractMicrosecond
	case "DATE_EXTRACT_MILLISECOND":
		return DateExtractMillisecond
	case "DATE_EXTRACT_SECOND":
		return DateExtractSecond
	case "DATE_EXTRACT_MINUTE":
		return DateExtractMinute
	case "DATE_EXTRACT_HOUR":
		return DateExtractHour
	case "DATE_EXTRACT_DAY":
		return DateExtractDay
	case "DATE_EXTRACT_DOW":
		return DateExtractDOW
	case "DATE_EXTRACT_DOY":
		return DateExtractDOY
	case "DATE_EXTRACT_MONTH":
		return DateExtractMonth
	case "DATE_EXTRACT_QUARTER":
		return DateExtractQuarter
	case "DATE_EXTRACT_YEAR":
		return DateExtractYear
	case "DATE_TRUNC_MICROSECOND":
		return DateTruncMicrosecond
	case "DATE_TRUNC_MILLISECOND":
		return DateTruncMillisecond
	case "DATE_TRUNC_SECOND":
		return DateTruncSecond
	case "DATE_TRUNC_MINUTE":
		return DateTruncMinute
	case "DATE_TRUNC_HOUR":
		return DateTruncHour
	case "DATE_TRUNC_DAY":
		return DateTruncDay
	case "DATE_TRUNC_DOW":
		return DateTruncDOW
	case "DATE_TRUNC_MONTH":
		return DateTruncMonth
	case "DATE_TRUNC_QUARTER":
		return DateTruncQuarter
	case "DATE_TRUNC_YEAR":
		return DateTruncYear
	case "TO_UNIX_EPOCH":
		return ToUnixEpoch
	case "TO_UNIX_MICRO":
		return ToUnixMicro
	case "GEO_HASH":
		return GeoHash
	case "GEO_TILE_X":
		return GeoTileX
	case "GEO_TILE_Y":
		return GeoTileY
	case "GEO_TILE_ES":
		return GeoTileES
	case "GEO_DISTANCE":
		return GeoDistance
	case "SIZE":
		return ObjectSize
	case "TABLE_GLOB":
		return TableGlob
	case "TABLE_PATTERN":
		return TablePattern
	case "IN_SUBQUERY":
		return InSubquery
	case "HASH_LOOKUP":
		return HashLookup
	case "IN_REPLACEMENT":
		return InReplacement
	case "HASH_REPLACEMENT":
		return HashReplacement
	case "SCALAR_REPLACEMENT":
		return ScalarReplacement
	case "STRUCT_REPLACEMENT":
		return StructReplacement
	case "LIST_REPLACEMENT":
		return ListReplacement
	case "TIME_BUCKET":
		return TimeBucket
	case "MAKE_LIST":
		return MakeList
	case "MAKE_STRUCT":
		return MakeStruct
	case "TYPE_BIT":
		return TypeBit
	}
	return Unspecified
}
