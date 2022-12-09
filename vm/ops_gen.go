package vm

// Code generated automatically; DO NOT EDIT

const (
	opret                     bcop = 0
	opjz                      bcop = 1
	oploadk                   bcop = 2
	opsavek                   bcop = 3
	opxchgk                   bcop = 4
	oploadb                   bcop = 5
	opsaveb                   bcop = 6
	oploadv                   bcop = 7
	opsavev                   bcop = 8
	oploadzerov               bcop = 9
	opsavezerov               bcop = 10
	oploadpermzerov           bcop = 11
	oploads                   bcop = 12
	opsaves                   bcop = 13
	oploadzeros               bcop = 14
	opsavezeros               bcop = 15
	opbroadcastimmk           bcop = 16
	opfalse                   bcop = 17
	opandk                    bcop = 18
	opork                     bcop = 19
	opandnotk                 bcop = 20
	opnandk                   bcop = 21
	opxork                    bcop = 22
	opnotk                    bcop = 23
	opxnork                   bcop = 24
	opbroadcastimmf           bcop = 25
	opbroadcastimmi           bcop = 26
	opabsf                    bcop = 27
	opabsi                    bcop = 28
	opnegf                    bcop = 29
	opnegi                    bcop = 30
	opsignf                   bcop = 31
	opsigni                   bcop = 32
	opsquaref                 bcop = 33
	opsquarei                 bcop = 34
	opbitnoti                 bcop = 35
	opbitcounti               bcop = 36
	oproundf                  bcop = 37
	oproundevenf              bcop = 38
	optruncf                  bcop = 39
	opfloorf                  bcop = 40
	opceilf                   bcop = 41
	opaddf                    bcop = 42
	opaddimmf                 bcop = 43
	opaddi                    bcop = 44
	opaddimmi                 bcop = 45
	opsubf                    bcop = 46
	opsubimmf                 bcop = 47
	opsubi                    bcop = 48
	opsubimmi                 bcop = 49
	oprsubf                   bcop = 50
	oprsubimmf                bcop = 51
	oprsubi                   bcop = 52
	oprsubimmi                bcop = 53
	opmulf                    bcop = 54
	opmulimmf                 bcop = 55
	opmuli                    bcop = 56
	opmulimmi                 bcop = 57
	opdivf                    bcop = 58
	opdivimmf                 bcop = 59
	oprdivf                   bcop = 60
	oprdivimmf                bcop = 61
	opdivi                    bcop = 62
	opdivimmi                 bcop = 63
	oprdivi                   bcop = 64
	oprdivimmi                bcop = 65
	opmodf                    bcop = 66
	opmodimmf                 bcop = 67
	oprmodf                   bcop = 68
	oprmodimmf                bcop = 69
	opmodi                    bcop = 70
	opmodimmi                 bcop = 71
	oprmodi                   bcop = 72
	oprmodimmi                bcop = 73
	opaddmulimmi              bcop = 74
	opminvaluef               bcop = 75
	opminvalueimmf            bcop = 76
	opmaxvaluef               bcop = 77
	opmaxvalueimmf            bcop = 78
	opminvaluei               bcop = 79
	opminvalueimmi            bcop = 80
	opmaxvaluei               bcop = 81
	opmaxvalueimmi            bcop = 82
	opandi                    bcop = 83
	opandimmi                 bcop = 84
	opori                     bcop = 85
	oporimmi                  bcop = 86
	opxori                    bcop = 87
	opxorimmi                 bcop = 88
	opslli                    bcop = 89
	opsllimmi                 bcop = 90
	opsrai                    bcop = 91
	opsraimmi                 bcop = 92
	opsrli                    bcop = 93
	opsrlimmi                 bcop = 94
	opsqrtf                   bcop = 95
	opcbrtf                   bcop = 96
	opexpf                    bcop = 97
	opexp2f                   bcop = 98
	opexp10f                  bcop = 99
	opexpm1f                  bcop = 100
	oplnf                     bcop = 101
	opln1pf                   bcop = 102
	oplog2f                   bcop = 103
	oplog10f                  bcop = 104
	opsinf                    bcop = 105
	opcosf                    bcop = 106
	optanf                    bcop = 107
	opasinf                   bcop = 108
	opacosf                   bcop = 109
	opatanf                   bcop = 110
	opatan2f                  bcop = 111
	ophypotf                  bcop = 112
	oppowf                    bcop = 113
	opcvtktof64               bcop = 114
	opcvtf64tok               bcop = 115
	opcvtktoi64               bcop = 116
	opcvti64tok               bcop = 117
	opcvti64tof64             bcop = 118
	opcvtf64toi64             bcop = 119
	opfproundu                bcop = 120
	opfproundd                bcop = 121
	opcvti64tostr             bcop = 122
	opsortcmpvnf              bcop = 123
	opsortcmpvnl              bcop = 124
	opcmpv                    bcop = 125
	opcmpvk                   bcop = 126
	opcmpvimmk                bcop = 127
	opcmpvi64                 bcop = 128
	opcmpvimmi64              bcop = 129
	opcmpvf64                 bcop = 130
	opcmpvimmf64              bcop = 131
	opcmpltstr                bcop = 132
	opcmplestr                bcop = 133
	opcmpgtstr                bcop = 134
	opcmpgestr                bcop = 135
	opcmpltk                  bcop = 136
	opcmpltimmk               bcop = 137
	opcmplek                  bcop = 138
	opcmpleimmk               bcop = 139
	opcmpgtk                  bcop = 140
	opcmpgtimmk               bcop = 141
	opcmpgek                  bcop = 142
	opcmpgeimmk               bcop = 143
	opcmpeqf                  bcop = 144
	opcmpeqimmf               bcop = 145
	opcmpltf                  bcop = 146
	opcmpltimmf               bcop = 147
	opcmplef                  bcop = 148
	opcmpleimmf               bcop = 149
	opcmpgtf                  bcop = 150
	opcmpgtimmf               bcop = 151
	opcmpgef                  bcop = 152
	opcmpgeimmf               bcop = 153
	opcmpeqi                  bcop = 154
	opcmpeqimmi               bcop = 155
	opcmplti                  bcop = 156
	opcmpltimmi               bcop = 157
	opcmplei                  bcop = 158
	opcmpleimmi               bcop = 159
	opcmpgti                  bcop = 160
	opcmpgtimmi               bcop = 161
	opcmpgei                  bcop = 162
	opcmpgeimmi               bcop = 163
	opisnanf                  bcop = 164
	opchecktag                bcop = 165
	optypebits                bcop = 166
	opisnull                  bcop = 167
	opisnotnull               bcop = 168
	opistrue                  bcop = 169
	opisfalse                 bcop = 170
	opeqslice                 bcop = 171
	opequalv                  bcop = 172
	opeqv4mask                bcop = 173
	opeqv4maskplus            bcop = 174
	opeqv8                    bcop = 175
	opeqv8plus                bcop = 176
	opleneq                   bcop = 177
	opdateaddmonth            bcop = 178
	opdateaddmonthimm         bcop = 179
	opdateaddyear             bcop = 180
	opdateaddquarter          bcop = 181
	opdatediffparam           bcop = 182
	opdatediffmonthyear       bcop = 183
	opdateextractmicrosecond  bcop = 184
	opdateextractmillisecond  bcop = 185
	opdateextractsecond       bcop = 186
	opdateextractminute       bcop = 187
	opdateextracthour         bcop = 188
	opdateextractday          bcop = 189
	opdateextractdow          bcop = 190
	opdateextractdoy          bcop = 191
	opdateextractmonth        bcop = 192
	opdateextractquarter      bcop = 193
	opdateextractyear         bcop = 194
	opdatetounixepoch         bcop = 195
	opdatetruncmillisecond    bcop = 196
	opdatetruncsecond         bcop = 197
	opdatetruncminute         bcop = 198
	opdatetrunchour           bcop = 199
	opdatetruncday            bcop = 200
	opdatetruncdow            bcop = 201
	opdatetruncmonth          bcop = 202
	opdatetruncquarter        bcop = 203
	opdatetruncyear           bcop = 204
	opunboxts                 bcop = 205
	opboxts                   bcop = 206
	optimelt                  bcop = 207
	optimegt                  bcop = 208
	opconsttm                 bcop = 209
	optmextract               bcop = 210
	opwidthbucketf            bcop = 211
	opwidthbucketi            bcop = 212
	optimebucketts            bcop = 213
	opgeohash                 bcop = 214
	opgeohashimm              bcop = 215
	opgeotilex                bcop = 216
	opgeotiley                bcop = 217
	opgeotilees               bcop = 218
	opgeotileesimm            bcop = 219
	opgeodistance             bcop = 220
	opconcatlenget1           bcop = 221
	opconcatlenget2           bcop = 222
	opconcatlenget3           bcop = 223
	opconcatlenget4           bcop = 224
	opconcatlenacc1           bcop = 225
	opconcatlenacc2           bcop = 226
	opconcatlenacc3           bcop = 227
	opconcatlenacc4           bcop = 228
	opallocstr                bcop = 229
	opappendstr               bcop = 230
	opfindsym                 bcop = 231
	opfindsym2                bcop = 232
	opfindsym2rev             bcop = 233
	opfindsym3                bcop = 234
	opblendv                  bcop = 235
	opblendrevv               bcop = 236
	opblendnum                bcop = 237
	opblendnumrev             bcop = 238
	opblendslice              bcop = 239
	opblendslicerev           bcop = 240
	opunpack                  bcop = 241
	opunsymbolize             bcop = 242
	opunboxktoi64             bcop = 243
	opunboxcoercef64          bcop = 244
	opunboxcoercei64          bcop = 245
	opunboxcvtf64             bcop = 246
	opunboxcvti64             bcop = 247
	optoint                   bcop = 248
	optof64                   bcop = 249
	opboxfloat                bcop = 250
	opboxint                  bcop = 251
	opboxmask                 bcop = 252
	opboxmask2                bcop = 253
	opboxmask3                bcop = 254
	opboxstring               bcop = 255
	opboxlist                 bcop = 256
	opmakelist                bcop = 257
	opmakestruct              bcop = 258
	ophashvalue               bcop = 259
	ophashvalueplus           bcop = 260
	ophashmember              bcop = 261
	ophashlookup              bcop = 262
	opaggandk                 bcop = 263
	opaggork                  bcop = 264
	opaggsumf                 bcop = 265
	opaggsumi                 bcop = 266
	opaggminf                 bcop = 267
	opaggmini                 bcop = 268
	opaggmaxf                 bcop = 269
	opaggmaxi                 bcop = 270
	opaggandi                 bcop = 271
	opaggori                  bcop = 272
	opaggxori                 bcop = 273
	opaggcount                bcop = 274
	opaggbucket               bcop = 275
	opaggslotandk             bcop = 276
	opaggslotork              bcop = 277
	opaggslotaddf             bcop = 278
	opaggslotaddi             bcop = 279
	opaggslotavgf             bcop = 280
	opaggslotavgi             bcop = 281
	opaggslotminf             bcop = 282
	opaggslotmini             bcop = 283
	opaggslotmaxf             bcop = 284
	opaggslotmaxi             bcop = 285
	opaggslotandi             bcop = 286
	opaggslotori              bcop = 287
	opaggslotxori             bcop = 288
	opaggslotcount            bcop = 289
	oplitref                  bcop = 290
	opauxval                  bcop = 291
	opsplit                   bcop = 292
	optuple                   bcop = 293
	opdupv                    bcop = 294
	opzerov                   bcop = 295
	opobjectsize              bcop = 296
	opCmpStrEqCs              bcop = 297
	opCmpStrEqCi              bcop = 298
	opCmpStrEqUTF8Ci          bcop = 299
	opCmpStrFuzzyA3           bcop = 300
	opCmpStrFuzzyUnicodeA3    bcop = 301
	opHasSubstrFuzzyA3        bcop = 302
	opHasSubstrFuzzyUnicodeA3 bcop = 303
	opSkip1charLeft           bcop = 304
	opSkip1charRight          bcop = 305
	opSkipNcharLeft           bcop = 306
	opSkipNcharRight          bcop = 307
	opTrimWsLeft              bcop = 308
	opTrimWsRight             bcop = 309
	opTrim4charLeft           bcop = 310
	opTrim4charRight          bcop = 311
	opContainsSuffixCs        bcop = 312
	opContainsSuffixCi        bcop = 313
	opContainsSuffixUTF8Ci    bcop = 314
	opContainsPrefixCs        bcop = 315
	opContainsPrefixCi        bcop = 316
	opContainsPrefixUTF8Ci    bcop = 317
	opLengthStr               bcop = 318
	opSubstr                  bcop = 319
	opSplitPart               bcop = 320
	opContainsSubstrCs        bcop = 321
	opContainsSubstrCi        bcop = 322
	opContainsSubstrUTF8Ci    bcop = 323
	opContainsPatternCs       bcop = 324
	opContainsPatternCi       bcop = 325
	opContainsPatternUTF8Ci   bcop = 326
	opIsSubnetOfIP4           bcop = 327
	opDfaT6                   bcop = 328
	opDfaT7                   bcop = 329
	opDfaT8                   bcop = 330
	opDfaT6Z                  bcop = 331
	opDfaT7Z                  bcop = 332
	opDfaT8Z                  bcop = 333
	opDfaLZ                   bcop = 334
	opslower                  bcop = 335
	opsupper                  bcop = 336
	opsadjustsize             bcop = 337
	opaggapproxcount          bcop = 338
	opaggapproxcountmerge     bcop = 339
	opaggslotapproxcount      bcop = 340
	opaggslotapproxcountmerge bcop = 341
	optrap                    bcop = 342
	_maxbcop                       = 343
)
