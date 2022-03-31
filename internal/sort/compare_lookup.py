"""
Produces lookup for compare_tuple.go.
"""

NULL       = 0x00
BOOL       = 0x01
UINT       = 0x02
INT        = 0x03
FLOAT      = 0x04
DECIMAL    = 0x05
TIMESTAMP  = 0x06
SYMBOL     = 0x07
STRING     = 0x08
CLOB       = 0x09
BLOB       = 0x0a
LIST       = 0x0b
SEXP       = 0x0c
STRUCT     = 0x0d
ANNOTATION = 0x0e
RESERVED   = 0x0f

strict_less_lookup = {
    BOOL : {UINT, INT, FLOAT, DECIMAL, TIMESTAMP, SYMBOL, STRING, CLOB, BLOB, LIST, SEXP, STRUCT},
    UINT : {TIMESTAMP, SYMBOL, STRING, CLOB, BLOB, LIST, SEXP, STRUCT},
    INT : {UINT, TIMESTAMP, SYMBOL, STRING, CLOB, BLOB, LIST, SEXP, STRUCT},
    FLOAT : {TIMESTAMP, SYMBOL, STRING, CLOB, BLOB, LIST, SEXP, STRUCT},
    DECIMAL : {TIMESTAMP, SYMBOL, STRING, CLOB, BLOB, LIST, SEXP, STRUCT},
    TIMESTAMP : {SYMBOL, STRING, CLOB, BLOB, LIST, SEXP, STRUCT},
    STRING : {STRING, CLOB, BLOB, LIST, SEXP, STRUCT},
    CLOB : {LIST, SEXP, STRUCT},
    BLOB : {LIST, SEXP, STRUCT},
    LIST : {STRUCT},
    SEXP : {STRUCT},
}

Less = "alwaysLess"
Greater = "alwaysGreater"
CompareSameType = "compareSameType"
CompareDifferentTypes = "compareDifferentTypes"
Invalid = "unsupportedRelation"


def not_supported(t):
    return t in (SYMBOL, SEXP, STRUCT, ANNOTATION, RESERVED)


def numeric(t):
    return t in (UINT, INT, FLOAT, DECIMAL)


def strict_less_aux(type1, type2):
    try:
        return type2 in strict_less_lookup[type1]
    except KeyError:
        return False


def relation(type1, type2):
    if not_supported(type1) or not_supported(type2):
        return Invalid

    if type1 == type2:
        return CompareSameType

    if strict_less_aux(type1, type2):
        return Less
    elif strict_less_aux(type2, type1):
        return Greater
    else:
        return CompareDifferentTypes


def relations():
    for type1 in range(16):
        for type2 in range(16):
            yield relation(type1, type2)


def main():
    r = list(relations())
    go = "var typesRelation [256]TypesRelation{%s}" % (', '.join(r))
    print(go)


if __name__ == '__main__':
    main()
