package vm

// Code generated automatically; DO NOT EDIT

const (
	optrap                    bcop = 0
	opret                     bcop = 1
	opretk                    bcop = 2
	opretbk                   bcop = 3
	opretsk                   bcop = 4
	opretbhk                  bcop = 5
	opjz                      bcop = 6
	opinit                    bcop = 7
	oploadpermzerov           bcop = 8
	opbroadcast0k             bcop = 9
	opbroadcast1k             bcop = 10
	opfalse                   bcop = 11
	opnotk                    bcop = 12
	opandk                    bcop = 13
	opnandk                   bcop = 14
	opandnk                   bcop = 15
	opork                     bcop = 16
	opxork                    bcop = 17
	opxnork                   bcop = 18
	opcvtktof64               bcop = 19
	opcvtktoi64               bcop = 20
	opcvti64tok               bcop = 21
	opcvtf64tok               bcop = 22
	opcvti64tof64             bcop = 23
	opcvttruncf64toi64        bcop = 24
	opcvtfloorf64toi64        bcop = 25
	opcvtceilf64toi64         bcop = 26
	opcvti64tostr             bcop = 27
	opsortcmpvnf              bcop = 28
	opsortcmpvnl              bcop = 29
	opcmpv                    bcop = 30
	opcmpvk                   bcop = 31
	opcmpvkimm                bcop = 32
	opcmpvi64                 bcop = 33
	opcmpvi64imm              bcop = 34
	opcmpvf64                 bcop = 35
	opcmpvf64imm              bcop = 36
	opcmpltstr                bcop = 37
	opcmplestr                bcop = 38
	opcmpgtstr                bcop = 39
	opcmpgestr                bcop = 40
	opcmpltk                  bcop = 41
	opcmpltkimm               bcop = 42
	opcmplek                  bcop = 43
	opcmplekimm               bcop = 44
	opcmpgtk                  bcop = 45
	opcmpgtkimm               bcop = 46
	opcmpgek                  bcop = 47
	opcmpgekimm               bcop = 48
	opcmpeqf64                bcop = 49
	opcmpeqf64imm             bcop = 50
	opcmpltf64                bcop = 51
	opcmpltf64imm             bcop = 52
	opcmplef64                bcop = 53
	opcmplef64imm             bcop = 54
	opcmpgtf64                bcop = 55
	opcmpgtf64imm             bcop = 56
	opcmpgef64                bcop = 57
	opcmpgef64imm             bcop = 58
	opcmpeqi64                bcop = 59
	opcmpeqi64imm             bcop = 60
	opcmplti64                bcop = 61
	opcmplti64imm             bcop = 62
	opcmplei64                bcop = 63
	opcmplei64imm             bcop = 64
	opcmpgti64                bcop = 65
	opcmpgti64imm             bcop = 66
	opcmpgei64                bcop = 67
	opcmpgei64imm             bcop = 68
	opisnanf                  bcop = 69
	opchecktag                bcop = 70
	optypebits                bcop = 71
	opisnullv                 bcop = 72
	opisnotnullv              bcop = 73
	opistruev                 bcop = 74
	opisfalsev                bcop = 75
	opcmpeqslice              bcop = 76
	opcmpeqv                  bcop = 77
	opcmpeqvimm               bcop = 78
	opdateaddmonth            bcop = 79
	opdateaddmonthimm         bcop = 80
	opdateaddyear             bcop = 81
	opdateaddquarter          bcop = 82
	opdatediffmicrosecond     bcop = 83
	opdatediffparam           bcop = 84
	opdatediffmqy             bcop = 85
	opdateextractmicrosecond  bcop = 86
	opdateextractmillisecond  bcop = 87
	opdateextractsecond       bcop = 88
	opdateextractminute       bcop = 89
	opdateextracthour         bcop = 90
	opdateextractday          bcop = 91
	opdateextractdow          bcop = 92
	opdateextractdoy          bcop = 93
	opdateextractmonth        bcop = 94
	opdateextractquarter      bcop = 95
	opdateextractyear         bcop = 96
	opdatetounixepoch         bcop = 97
	opdatetounixmicro         bcop = 98
	opdatetruncmillisecond    bcop = 99
	opdatetruncsecond         bcop = 100
	opdatetruncminute         bcop = 101
	opdatetrunchour           bcop = 102
	opdatetruncday            bcop = 103
	opdatetruncdow            bcop = 104
	opdatetruncmonth          bcop = 105
	opdatetruncquarter        bcop = 106
	opdatetruncyear           bcop = 107
	opunboxts                 bcop = 108
	opboxts                   bcop = 109
	opwidthbucketf64          bcop = 110
	opwidthbucketi64          bcop = 111
	optimebucketts            bcop = 112
	opgeohash                 bcop = 113
	opgeohashimm              bcop = 114
	opgeotilex                bcop = 115
	opgeotiley                bcop = 116
	opgeotilees               bcop = 117
	opgeotileesimm            bcop = 118
	opgeodistance             bcop = 119
	opalloc                   bcop = 120
	opconcatstr               bcop = 121
	opfindsym                 bcop = 122
	opfindsym2                bcop = 123
	opblendv                  bcop = 124
	opblendk                  bcop = 125
	opblendi64                bcop = 126
	opblendf64                bcop = 127
	opblendslice              bcop = 128
	opunpack                  bcop = 129
	opunsymbolize             bcop = 130
	opunboxktoi64             bcop = 131
	opunboxcoercef64          bcop = 132
	opunboxcoercei64          bcop = 133
	opunboxcvtf64             bcop = 134
	opunboxcvti64             bcop = 135
	opboxf64                  bcop = 136
	opboxi64                  bcop = 137
	opboxk                    bcop = 138
	opboxstr                  bcop = 139
	opboxlist                 bcop = 140
	opmakelist                bcop = 141
	opmakestruct              bcop = 142
	ophashvalue               bcop = 143
	ophashvalueplus           bcop = 144
	ophashmember              bcop = 145
	ophashlookup              bcop = 146
	opaggandk                 bcop = 147
	opaggork                  bcop = 148
	opaggsumf                 bcop = 149
	opaggsumi                 bcop = 150
	opaggminf                 bcop = 151
	opaggmini                 bcop = 152
	opaggmaxf                 bcop = 153
	opaggmaxi                 bcop = 154
	opaggandi                 bcop = 155
	opaggori                  bcop = 156
	opaggxori                 bcop = 157
	opaggcount                bcop = 158
	opaggbucket               bcop = 159
	opaggslotandk             bcop = 160
	opaggslotork              bcop = 161
	opaggslotaddf             bcop = 162
	opaggslotaddi             bcop = 163
	opaggslotavgf             bcop = 164
	opaggslotavgi             bcop = 165
	opaggslotminf             bcop = 166
	opaggslotmini             bcop = 167
	opaggslotmaxf             bcop = 168
	opaggslotmaxi             bcop = 169
	opaggslotandi             bcop = 170
	opaggslotori              bcop = 171
	opaggslotxori             bcop = 172
	opaggslotcount            bcop = 173
	oplitref                  bcop = 174
	opauxval                  bcop = 175
	opsplit                   bcop = 176
	optuple                   bcop = 177
	opmovk                    bcop = 178
	opzerov                   bcop = 179
	opmovv                    bcop = 180
	opmovvk                   bcop = 181
	opmovf64                  bcop = 182
	opmovi64                  bcop = 183
	opobjectsize              bcop = 184
	opCmpStrEqCs              bcop = 185
	opCmpStrEqCi              bcop = 186
	opCmpStrEqUTF8Ci          bcop = 187
	opCmpStrFuzzyA3           bcop = 188
	opCmpStrFuzzyUnicodeA3    bcop = 189
	opHasSubstrFuzzyA3        bcop = 190
	opHasSubstrFuzzyUnicodeA3 bcop = 191
	opSkip1charLeft           bcop = 192
	opSkip1charRight          bcop = 193
	opSkipNcharLeft           bcop = 194
	opSkipNcharRight          bcop = 195
	opTrimWsLeft              bcop = 196
	opTrimWsRight             bcop = 197
	opTrim4charLeft           bcop = 198
	opTrim4charRight          bcop = 199
	opLengthStr               bcop = 200
	opSubstr                  bcop = 201
	opSplitPart               bcop = 202
	opContainsPrefixCs        bcop = 203
	opContainsPrefixCi        bcop = 204
	opContainsPrefixUTF8Ci    bcop = 205
	opContainsSuffixCs        bcop = 206
	opContainsSuffixCi        bcop = 207
	opContainsSuffixUTF8Ci    bcop = 208
	opContainsSubstrCs        bcop = 209
	opContainsSubstrCi        bcop = 210
	opContainsSubstrUTF8Ci    bcop = 211
	opEqPatternCs             bcop = 212
	opEqPatternCi             bcop = 213
	opEqPatternUTF8Ci         bcop = 214
	opContainsPatternCs       bcop = 215
	opContainsPatternCi       bcop = 216
	opContainsPatternUTF8Ci   bcop = 217
	opIsSubnetOfIP4           bcop = 218
	opDfaT6                   bcop = 219
	opDfaT7                   bcop = 220
	opDfaT8                   bcop = 221
	opDfaT6Z                  bcop = 222
	opDfaT7Z                  bcop = 223
	opDfaT8Z                  bcop = 224
	opDfaLZ                   bcop = 225
	opslower                  bcop = 226
	opsupper                  bcop = 227
	opaggapproxcount          bcop = 228
	opaggapproxcountmerge     bcop = 229
	opaggslotapproxcount      bcop = 230
	opaggslotapproxcountmerge bcop = 231
	opbroadcasti64            bcop = 232
	opabsi64                  bcop = 233
	opnegi64                  bcop = 234
	opsigni64                 bcop = 235
	opsquarei64               bcop = 236
	opbitnoti64               bcop = 237
	opbitcounti64             bcop = 238
	opaddi64                  bcop = 239
	opaddi64imm               bcop = 240
	opsubi64                  bcop = 241
	opsubi64imm               bcop = 242
	oprsubi64imm              bcop = 243
	opmuli64                  bcop = 244
	opmuli64imm               bcop = 245
	opdivi64                  bcop = 246
	opdivi64imm               bcop = 247
	oprdivi64imm              bcop = 248
	opmodi64                  bcop = 249
	opmodi64imm               bcop = 250
	oprmodi64imm              bcop = 251
	opaddmuli64imm            bcop = 252
	opminvaluei64             bcop = 253
	opminvaluei64imm          bcop = 254
	opmaxvaluei64             bcop = 255
	opmaxvaluei64imm          bcop = 256
	opandi64                  bcop = 257
	opandi64imm               bcop = 258
	opori64                   bcop = 259
	opori64imm                bcop = 260
	opxori64                  bcop = 261
	opxori64imm               bcop = 262
	opslli64                  bcop = 263
	opslli64imm               bcop = 264
	opsrai64                  bcop = 265
	opsrai64imm               bcop = 266
	opsrli64                  bcop = 267
	opsrli64imm               bcop = 268
	opbroadcastf64            bcop = 269
	opabsf64                  bcop = 270
	opnegf64                  bcop = 271
	opsignf64                 bcop = 272
	opsquaref64               bcop = 273
	oproundf64                bcop = 274
	oproundevenf64            bcop = 275
	optruncf64                bcop = 276
	opfloorf64                bcop = 277
	opceilf64                 bcop = 278
	opaddf64                  bcop = 279
	opaddf64imm               bcop = 280
	opsubf64                  bcop = 281
	opsubf64imm               bcop = 282
	oprsubf64imm              bcop = 283
	opmulf64                  bcop = 284
	opmulf64imm               bcop = 285
	opdivf64                  bcop = 286
	opdivf64imm               bcop = 287
	oprdivf64imm              bcop = 288
	opmodf64                  bcop = 289
	opmodf64imm               bcop = 290
	oprmodf64imm              bcop = 291
	opminvaluef64             bcop = 292
	opminvaluef64imm          bcop = 293
	opmaxvaluef64             bcop = 294
	opmaxvaluef64imm          bcop = 295
	opsqrtf64                 bcop = 296
	opcbrtf64                 bcop = 297
	opexpf64                  bcop = 298
	opexp2f64                 bcop = 299
	opexp10f64                bcop = 300
	opexpm1f64                bcop = 301
	oplnf64                   bcop = 302
	opln1pf64                 bcop = 303
	oplog2f64                 bcop = 304
	oplog10f64                bcop = 305
	opsinf64                  bcop = 306
	opcosf64                  bcop = 307
	optanf64                  bcop = 308
	opasinf64                 bcop = 309
	opacosf64                 bcop = 310
	opatanf64                 bcop = 311
	opatan2f64                bcop = 312
	ophypotf64                bcop = 313
	oppowf64                  bcop = 314
	_maxbcop                       = 315
)
