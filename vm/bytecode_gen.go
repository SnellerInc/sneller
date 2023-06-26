package vm

// Code generated automatically; DO NOT EDIT

var opinfo = [_maxbcop]bcopinfo{
	optrap:                    {text: "trap"},
	opbroadcasti64:            {text: "broadcast.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[0:1] /* {bcImmI64} */},
	opabsi64:                  {text: "abs.i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opnegi64:                  {text: "neg.i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opsigni64:                 {text: "sign.i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opsquarei64:               {text: "square.i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opbitnoti64:               {text: "bitnot.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opbitcounti64:             {text: "bitcount.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opbitcounti64v2:           {text: "bitcount.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opaddi64:                  {text: "add.i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opaddi64imm:               {text: "add.i64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opsubi64:                  {text: "sub.i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opsubi64imm:               {text: "sub.i64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	oprsubi64imm:              {text: "rsub.i64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opmuli64:                  {text: "mul.i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opmuli64imm:               {text: "mul.i64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opdivi64:                  {text: "div.i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opdivi64imm:               {text: "div.i64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	oprdivi64imm:              {text: "rdiv.i64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opmodi64:                  {text: "mod.i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opmodi64imm:               {text: "mod.i64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	oprmodi64imm:              {text: "rmod.i64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	oppmodi64:                 {text: "pmod.i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	oppmodi64imm:              {text: "pmod.i64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	oprpmodi64imm:             {text: "rpmod.i64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opaddmuli64imm:            {text: "addmul.i64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[25:29] /* {bcS, bcS, bcImmI64, bcK} */},
	opminvaluei64:             {text: "minvalue.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opminvaluei64imm:          {text: "minvalue.i64@imm", out: bcargs[1:2] /* {bcS} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opmaxvaluei64:             {text: "maxvalue.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opmaxvaluei64imm:          {text: "maxvalue.i64@imm", out: bcargs[1:2] /* {bcS} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opandi64:                  {text: "and.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opandi64imm:               {text: "and.i64@imm", out: bcargs[1:2] /* {bcS} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opori64:                   {text: "or.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opori64imm:                {text: "or.i64@imm", out: bcargs[1:2] /* {bcS} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opxori64:                  {text: "xor.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opxori64imm:               {text: "xor.i64@imm", out: bcargs[1:2] /* {bcS} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opslli64:                  {text: "sll.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opslli64imm:               {text: "sll.i64@imm", out: bcargs[1:2] /* {bcS} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opsrai64:                  {text: "sra.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opsrai64imm:               {text: "sra.i64@imm", out: bcargs[1:2] /* {bcS} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opsrli64:                  {text: "srl.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opsrli64imm:               {text: "srl.i64@imm", out: bcargs[1:2] /* {bcS} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opbroadcastf64:            {text: "broadcast.f64", out: bcargs[1:2] /* {bcS} */, in: bcargs[19:20] /* {bcImmF64} */},
	opabsf64:                  {text: "abs.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opnegf64:                  {text: "neg.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opsignf64:                 {text: "sign.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opsquaref64:               {text: "square.f64", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	oproundf64:                {text: "round.f64", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	oproundevenf64:            {text: "roundeven.f64", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	optruncf64:                {text: "trunc.f64", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opfloorf64:                {text: "floor.f64", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opceilf64:                 {text: "ceil.f64", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opaddf64:                  {text: "add.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opaddf64imm:               {text: "add.f64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	opsubf64:                  {text: "sub.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opsubf64imm:               {text: "sub.f64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	oprsubf64imm:              {text: "rsub.f64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	opmulf64:                  {text: "mul.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opmulf64imm:               {text: "mul.f64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	opdivf64:                  {text: "div.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opdivf64imm:               {text: "div.f64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	oprdivf64imm:              {text: "rdiv.f64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	opmodf64:                  {text: "mod.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opmodf64imm:               {text: "mod.f64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	oprmodf64imm:              {text: "rmod.f64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	oppmodf64:                 {text: "pmod.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	oppmodf64imm:              {text: "pmod.f64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	oprpmodf64imm:             {text: "rpmod.f64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	opminvaluef64:             {text: "minvalue.f64", out: bcargs[1:2] /* {bcS} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opminvaluef64imm:          {text: "minvalue.f64@imm", out: bcargs[1:2] /* {bcS} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	opmaxvaluef64:             {text: "maxvalue.f64", out: bcargs[1:2] /* {bcS} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opmaxvaluef64imm:          {text: "maxvalue.f64@imm", out: bcargs[1:2] /* {bcS} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	opsqrtf64:                 {text: "sqrt.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opcbrtf64:                 {text: "cbrt.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opexpf64:                  {text: "exp.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opexp2f64:                 {text: "exp2.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opexp10f64:                {text: "exp10.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opexpm1f64:                {text: "expm1.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	oplnf64:                   {text: "ln.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opln1pf64:                 {text: "ln1p.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	oplog2f64:                 {text: "log2.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	oplog10f64:                {text: "log10.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opsinf64:                  {text: "sin.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opcosf64:                  {text: "cos.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	optanf64:                  {text: "tan.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opasinf64:                 {text: "asin.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opacosf64:                 {text: "acos.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opatanf64:                 {text: "atan.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opatan2f64:                {text: "atan2.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	ophypotf64:                {text: "hypot.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	oppowf64:                  {text: "pow.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opret:                     {text: "ret"},
	opretk:                    {text: "ret.k", in: bcargs[3:4] /* {bcK} */},
	opretbk:                   {text: "ret.b.k", in: bcargs[51:53] /* {bcB, bcK} */},
	opretsk:                   {text: "ret.s.k", in: bcargs[2:4] /* {bcS, bcK} */},
	opretbhk:                  {text: "ret.b.h.k", in: bcargs[11:14] /* {bcB, bcH, bcK} */},
	opinit:                    {text: "init", out: bcargs[51:53] /* {bcB, bcK} */},
	opbroadcast0k:             {text: "broadcast0.k", out: bcargs[3:4] /* {bcK} */},
	opbroadcast1k:             {text: "broadcast1.k", out: bcargs[3:4] /* {bcK} */},
	opfalse:                   {text: "false.k", out: bcargs[9:11] /* {bcV, bcK} */},
	opnotk:                    {text: "not.k", out: bcargs[3:4] /* {bcK} */, in: bcargs[3:4] /* {bcK} */},
	opandk:                    {text: "and.k", out: bcargs[3:4] /* {bcK} */, in: bcargs[6:8] /* {bcK, bcK} */},
	opandnk:                   {text: "andn.k", out: bcargs[3:4] /* {bcK} */, in: bcargs[6:8] /* {bcK, bcK} */},
	opork:                     {text: "or.k", out: bcargs[3:4] /* {bcK} */, in: bcargs[6:8] /* {bcK, bcK} */},
	opxork:                    {text: "xor.k", out: bcargs[3:4] /* {bcK} */, in: bcargs[6:8] /* {bcK, bcK} */},
	opxnork:                   {text: "xnor.k", out: bcargs[3:4] /* {bcK} */, in: bcargs[6:8] /* {bcK, bcK} */},
	opcvtktof64:               {text: "cvt.ktof64", out: bcargs[1:2] /* {bcS} */, in: bcargs[3:4] /* {bcK} */},
	opcvtktoi64:               {text: "cvt.ktoi64", out: bcargs[1:2] /* {bcS} */, in: bcargs[3:4] /* {bcK} */},
	opcvti64tok:               {text: "cvt.i64tok", out: bcargs[3:4] /* {bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opcvtf64tok:               {text: "cvt.f64tok", out: bcargs[3:4] /* {bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opcvti64tof64:             {text: "cvt.i64tof64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opcvttruncf64toi64:        {text: "cvttrunc.f64toi64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opcvtfloorf64toi64:        {text: "cvtfloor.f64toi64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opcvtceilf64toi64:         {text: "cvtceil.f64toi64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opcvti64tostr:             {text: "cvt.i64tostr", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */, scratch: 20 * 16},
	opcmpv:                    {text: "cmpv", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[75:78] /* {bcV, bcV, bcK} */},
	opsortcmpvnf:              {text: "sortcmpv@nf", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[75:78] /* {bcV, bcV, bcK} */},
	opsortcmpvnl:              {text: "sortcmpv@nl", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[75:78] /* {bcV, bcV, bcK} */},
	opcmpvk:                   {text: "cmpv.k", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[69:72] /* {bcV, bcK, bcK} */},
	opcmpvkimm:                {text: "cmpv.k@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[58:61] /* {bcV, bcImmU16, bcK} */},
	opcmpvi64:                 {text: "cmpv.i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[72:75] /* {bcV, bcS, bcK} */},
	opcmpvi64imm:              {text: "cmpv.i64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[93:96] /* {bcV, bcImmI64, bcK} */},
	opcmpvf64:                 {text: "cmpv.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[72:75] /* {bcV, bcS, bcK} */},
	opcmpvf64imm:              {text: "cmpv.f64@imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[90:93] /* {bcV, bcImmF64, bcK} */},
	opcmpltstr:                {text: "cmplt.str", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmplestr:                {text: "cmple.str", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmpgtstr:                {text: "cmpgt.str", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmpgestr:                {text: "cmpge.str", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmpltk:                  {text: "cmplt.k", out: bcargs[3:4] /* {bcK} */, in: bcargs[37:40] /* {bcK, bcK, bcK} */},
	opcmpltkimm:               {text: "cmplt.k@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[39:42] /* {bcK, bcImmU16, bcK} */},
	opcmplek:                  {text: "cmple.k", out: bcargs[3:4] /* {bcK} */, in: bcargs[37:40] /* {bcK, bcK, bcK} */},
	opcmplekimm:               {text: "cmple.k@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[39:42] /* {bcK, bcImmU16, bcK} */},
	opcmpgtk:                  {text: "cmpgt.k", out: bcargs[3:4] /* {bcK} */, in: bcargs[37:40] /* {bcK, bcK, bcK} */},
	opcmpgtkimm:               {text: "cmpgt.k@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[39:42] /* {bcK, bcImmU16, bcK} */},
	opcmpgek:                  {text: "cmpge.k", out: bcargs[3:4] /* {bcK} */, in: bcargs[37:40] /* {bcK, bcK, bcK} */},
	opcmpgekimm:               {text: "cmpge.k@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[39:42] /* {bcK, bcImmU16, bcK} */},
	opcmpeqf64:                {text: "cmpeq.f64", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmpeqf64imm:             {text: "cmpeq.f64@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	opcmpltf64:                {text: "cmplt.f64", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmpltf64imm:             {text: "cmplt.f64@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	opcmplef64:                {text: "cmple.f64", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmplef64imm:             {text: "cmple.f64@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	opcmpgtf64:                {text: "cmpgt.f64", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmpgtf64imm:             {text: "cmpgt.f64@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	opcmpgef64:                {text: "cmpge.f64", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmpgef64imm:             {text: "cmpge.f64@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[18:21] /* {bcS, bcImmF64, bcK} */},
	opcmpeqi64:                {text: "cmpeq.i64", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmpeqi64imm:             {text: "cmpeq.i64@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opcmplti64:                {text: "cmplt.i64", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmplti64imm:             {text: "cmplt.i64@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opcmplei64:                {text: "cmple.i64", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmplei64imm:             {text: "cmple.i64@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opcmpgti64:                {text: "cmpgt.i64", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmpgti64imm:             {text: "cmpgt.i64@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opcmpgei64:                {text: "cmpge.i64", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmpgei64imm:             {text: "cmpge.i64@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opisnanf:                  {text: "isnan.f", out: bcargs[3:4] /* {bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opchecktag:                {text: "checktag", out: bcargs[9:11] /* {bcV, bcK} */, in: bcargs[58:61] /* {bcV, bcImmU16, bcK} */},
	optypebits:                {text: "typebits", out: bcargs[1:2] /* {bcS} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opisnullv:                 {text: "isnull.v", out: bcargs[3:4] /* {bcK} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opisnotnullv:              {text: "isnotnull.v", out: bcargs[3:4] /* {bcK} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opistruev:                 {text: "istrue.v", out: bcargs[3:4] /* {bcK} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opisfalsev:                {text: "isfalse.v", out: bcargs[3:4] /* {bcK} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opcmpeqslice:              {text: "cmpeq.slice", out: bcargs[3:4] /* {bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opcmpeqv:                  {text: "cmpeq.v", out: bcargs[3:4] /* {bcK} */, in: bcargs[75:78] /* {bcV, bcV, bcK} */},
	opcmpeqvimm:               {text: "cmpeq.v@imm", out: bcargs[3:4] /* {bcK} */, in: bcargs[33:36] /* {bcV, bcLitRef, bcK} */},
	opdateaddmonth:            {text: "dateaddmonth", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opdateaddmonthimm:         {text: "dateaddmonth.imm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
	opdateaddyear:             {text: "dateaddyear", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opdateaddquarter:          {text: "dateaddquarter", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opdatebin:                 {text: "datebin", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[0:4] /* {bcImmI64, bcS, bcS, bcK} */},
	opdatediffmicrosecond:     {text: "datediffmicrosecond", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opdatediffparam:           {text: "datediffparam", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[78:82] /* {bcS, bcS, bcImmU64, bcK} */},
	opdatediffmqy:             {text: "datediffmqy", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[14:18] /* {bcS, bcS, bcImmU16, bcK} */},
	opdateextractmicrosecond:  {text: "dateextractmicrosecond", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdateextractmillisecond:  {text: "dateextractmillisecond", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdateextractsecond:       {text: "dateextractsecond", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdateextractminute:       {text: "dateextractminute", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdateextracthour:         {text: "dateextracthour", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdateextractday:          {text: "dateextractday", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdateextractdow:          {text: "dateextractdow", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdateextractdoy:          {text: "dateextractdoy", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdateextractmonth:        {text: "dateextractmonth", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdateextractquarter:      {text: "dateextractquarter", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdateextractyear:         {text: "dateextractyear", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdatetounixepoch:         {text: "datetounixepoch", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdatetounixmicro:         {text: "datetounixmicro", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdatetruncmillisecond:    {text: "datetruncmillisecond", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdatetruncsecond:         {text: "datetruncsecond", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdatetruncminute:         {text: "datetruncminute", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdatetrunchour:           {text: "datetrunchour", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdatetruncday:            {text: "datetruncday", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdatetruncdow:            {text: "datetruncdow", out: bcargs[1:2] /* {bcS} */, in: bcargs[15:18] /* {bcS, bcImmU16, bcK} */},
	opdatetruncmonth:          {text: "datetruncmonth", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdatetruncquarter:        {text: "datetruncquarter", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opdatetruncyear:           {text: "datetruncyear", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opunboxts:                 {text: "unboxts", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opboxts:                   {text: "boxts", out: bcargs[9:10] /* {bcV} */, in: bcargs[2:4] /* {bcS, bcK} */, scratch: 16 * 16},
	opwidthbucketf64:          {text: "widthbucket.f64", out: bcargs[1:2] /* {bcS} */, in: bcargs[42:47] /* {bcS, bcS, bcS, bcS, bcK} */},
	opwidthbucketi64:          {text: "widthbucket.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[42:47] /* {bcS, bcS, bcS, bcS, bcK} */},
	optimebucketts:            {text: "timebucket.ts", out: bcargs[1:2] /* {bcS} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opgeohash:                 {text: "geohash", out: bcargs[1:2] /* {bcS} */, in: bcargs[43:47] /* {bcS, bcS, bcS, bcK} */, scratch: 16 * 16},
	opgeohashimm:              {text: "geohashimm", out: bcargs[1:2] /* {bcS} */, in: bcargs[14:18] /* {bcS, bcS, bcImmU16, bcK} */, scratch: 16 * 16},
	opgeotilex:                {text: "geotilex", out: bcargs[1:2] /* {bcS} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opgeotiley:                {text: "geotiley", out: bcargs[1:2] /* {bcS} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opgeotilees:               {text: "geotilees", out: bcargs[1:2] /* {bcS} */, in: bcargs[43:47] /* {bcS, bcS, bcS, bcK} */, scratch: 32 * 16},
	opgeotileesimm:            {text: "geotilees.imm", out: bcargs[1:2] /* {bcS} */, in: bcargs[14:18] /* {bcS, bcS, bcImmU16, bcK} */, scratch: 32 * 16},
	opgeodistance:             {text: "geodistance", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[42:47] /* {bcS, bcS, bcS, bcS, bcK} */},
	opalloc:                   {text: "alloc", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */, scratch: PageSize},
	opconcatstr:               {text: "concatstr", out: bcargs[2:4] /* {bcS, bcK} */, va: bcargs[2:4] /* {bcS, bcK} */, scratch: PageSize},
	opfindsym:                 {text: "findsym", out: bcargs[9:11] /* {bcV, bcK} */, in: bcargs[96:99] /* {bcB, bcSymbolID, bcK} */},
	opfindsym2:                {text: "findsym2", out: bcargs[9:11] /* {bcV, bcK} */, in: bcargs[53:58] /* {bcB, bcV, bcK, bcSymbolID, bcK} */},
	opblendv:                  {text: "blend.v", out: bcargs[9:11] /* {bcV, bcK} */, in: bcargs[67:71] /* {bcV, bcK, bcV, bcK} */},
	opblendf64:                {text: "blend.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[86:90] /* {bcS, bcK, bcS, bcK} */},
	opunpack:                  {text: "unpack", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[58:61] /* {bcV, bcImmU16, bcK} */},
	opunsymbolize:             {text: "unsymbolize", out: bcargs[9:10] /* {bcV} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opunboxktoi64:             {text: "unbox.k@i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opunboxcoercef64:          {text: "unbox.coerce.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opunboxcoercei64:          {text: "unbox.coerce.i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opunboxcvtf64:             {text: "unbox.cvt.f64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opunboxcvti64:             {text: "unbox.cvt.i64", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opboxf64:                  {text: "box.f64", out: bcargs[9:10] /* {bcV} */, in: bcargs[2:4] /* {bcS, bcK} */, scratch: 9 * 16},
	opboxi64:                  {text: "box.i64", out: bcargs[9:10] /* {bcV} */, in: bcargs[2:4] /* {bcS, bcK} */, scratch: 9 * 16},
	opboxk:                    {text: "box.k", out: bcargs[9:10] /* {bcV} */, in: bcargs[6:8] /* {bcK, bcK} */, scratch: 16},
	opboxstr:                  {text: "box.str", out: bcargs[9:10] /* {bcV} */, in: bcargs[2:4] /* {bcS, bcK} */, scratch: PageSize},
	opboxlist:                 {text: "box.list", out: bcargs[9:10] /* {bcV} */, in: bcargs[2:4] /* {bcS, bcK} */, scratch: PageSize},
	opmakelist:                {text: "makelist", out: bcargs[9:11] /* {bcV, bcK} */, in: bcargs[3:4] /* {bcK} */, va: bcargs[9:11] /* {bcV, bcK} */, scratch: PageSize},
	opmakestruct:              {text: "makestruct", out: bcargs[9:11] /* {bcV, bcK} */, in: bcargs[3:4] /* {bcK} */, va: bcargs[82:85] /* {bcSymbolID, bcV, bcK} */, scratch: PageSize},
	ophashvalue:               {text: "hashvalue", out: bcargs[8:9] /* {bcH} */, in: bcargs[9:11] /* {bcV, bcK} */},
	ophashvalueplus:           {text: "hashvalue+", out: bcargs[8:9] /* {bcH} */, in: bcargs[8:11] /* {bcH, bcV, bcK} */},
	ophashmember:              {text: "hashmember", out: bcargs[3:4] /* {bcK} */, in: bcargs[30:33] /* {bcH, bcImmU16, bcK} */},
	ophashlookup:              {text: "hashlookup", out: bcargs[9:11] /* {bcV, bcK} */, in: bcargs[30:33] /* {bcH, bcImmU16, bcK} */},
	opaggandk:                 {text: "aggand.k", in: bcargs[36:39] /* {bcAggSlot, bcK, bcK} */},
	opaggork:                  {text: "aggor.k", in: bcargs[36:39] /* {bcAggSlot, bcK, bcK} */},
	opaggslotsumf:             {text: "aggslotsum.f64", in: bcargs[61:65] /* {bcAggSlot, bcL, bcS, bcK} */},
	opaggsumf:                 {text: "aggsum.f64", in: bcargs[85:88] /* {bcAggSlot, bcS, bcK} */},
	opaggsumi:                 {text: "aggsum.i64", in: bcargs[85:88] /* {bcAggSlot, bcS, bcK} */},
	opaggminf:                 {text: "aggmin.f64", in: bcargs[85:88] /* {bcAggSlot, bcS, bcK} */},
	opaggmini:                 {text: "aggmin.i64", in: bcargs[85:88] /* {bcAggSlot, bcS, bcK} */},
	opaggmaxf:                 {text: "aggmax.f64", in: bcargs[85:88] /* {bcAggSlot, bcS, bcK} */},
	opaggmaxi:                 {text: "aggmax.i64", in: bcargs[85:88] /* {bcAggSlot, bcS, bcK} */},
	opaggandi:                 {text: "aggand.i64", in: bcargs[85:88] /* {bcAggSlot, bcS, bcK} */},
	opaggori:                  {text: "aggor.i64", in: bcargs[85:88] /* {bcAggSlot, bcS, bcK} */},
	opaggxori:                 {text: "aggxor.i64", in: bcargs[85:88] /* {bcAggSlot, bcS, bcK} */},
	opaggcount:                {text: "aggcount", in: bcargs[36:38] /* {bcAggSlot, bcK} */},
	opaggmergestate:           {text: "aggmergestate", in: bcargs[85:88] /* {bcAggSlot, bcS, bcK} */},
	opaggbucket:               {text: "aggbucket", out: bcargs[5:6] /* {bcL} */, in: bcargs[12:14] /* {bcH, bcK} */},
	opaggslotandk:             {text: "aggslotand.k", in: bcargs[4:8] /* {bcAggSlot, bcL, bcK, bcK} */},
	opaggslotork:              {text: "aggslotor.k", in: bcargs[4:8] /* {bcAggSlot, bcL, bcK, bcK} */},
	opaggslotsumi:             {text: "aggslotsum.i64", in: bcargs[61:65] /* {bcAggSlot, bcL, bcS, bcK} */},
	opaggslotavgf:             {text: "aggslotavg.f64", in: bcargs[61:65] /* {bcAggSlot, bcL, bcS, bcK} */},
	opaggslotavgi:             {text: "aggslotavg.i64", in: bcargs[61:65] /* {bcAggSlot, bcL, bcS, bcK} */},
	opaggslotminf:             {text: "aggslotmin.f64", in: bcargs[61:65] /* {bcAggSlot, bcL, bcS, bcK} */},
	opaggslotmini:             {text: "aggslotmin.i64", in: bcargs[61:65] /* {bcAggSlot, bcL, bcS, bcK} */},
	opaggslotmaxf:             {text: "aggslotmax.f64", in: bcargs[61:65] /* {bcAggSlot, bcL, bcS, bcK} */},
	opaggslotmaxi:             {text: "aggslotmax.i64", in: bcargs[61:65] /* {bcAggSlot, bcL, bcS, bcK} */},
	opaggslotandi:             {text: "aggslotand.i64", in: bcargs[61:65] /* {bcAggSlot, bcL, bcS, bcK} */},
	opaggslotori:              {text: "aggslotor.i64", in: bcargs[61:65] /* {bcAggSlot, bcL, bcS, bcK} */},
	opaggslotxori:             {text: "aggslotxor.i64", in: bcargs[61:65] /* {bcAggSlot, bcL, bcS, bcK} */},
	opaggslotcount:            {text: "aggslotcount", in: bcargs[4:7] /* {bcAggSlot, bcL, bcK} */},
	opaggslotcountv2:          {text: "aggslotcount", in: bcargs[4:7] /* {bcAggSlot, bcL, bcK} */},
	opaggslotmergestate:       {text: "aggslotmergestate", in: bcargs[61:65] /* {bcAggSlot, bcL, bcS, bcK} */},
	oplitref:                  {text: "litref", out: bcargs[9:10] /* {bcV} */, in: bcargs[34:35] /* {bcLitRef} */},
	opauxval:                  {text: "auxval", out: bcargs[9:11] /* {bcV, bcK} */, in: bcargs[65:66] /* {bcAuxSlot} */},
	opsplit:                   {text: "split", out: bcargs[72:75] /* {bcV, bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	optuple:                   {text: "tuple", out: bcargs[51:53] /* {bcB, bcK} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opmovk:                    {text: "mov.k", out: bcargs[3:4] /* {bcK} */, in: bcargs[3:4] /* {bcK} */},
	opzerov:                   {text: "zero.v", out: bcargs[9:10] /* {bcV} */},
	opmovv:                    {text: "mov.v", out: bcargs[9:10] /* {bcV} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opmovvk:                   {text: "mov.v.k", out: bcargs[9:11] /* {bcV, bcK} */, in: bcargs[9:11] /* {bcV, bcK} */},
	opmovf64:                  {text: "mov.f64", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opmovi64:                  {text: "mov.i64", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opobjectsize:              {text: "objectsize", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[9:11] /* {bcV, bcK} */},
	oparraysize:               {text: "arraysize", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	oparrayposition:           {text: "arrayposition", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[66:69] /* {bcS, bcV, bcK} */},
	oparraysum:                {text: "arraysum", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opvectorinnerproduct:      {text: "vectorinnerproduct", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opvectorinnerproductimm:   {text: "bcvectorinnerproductimm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opvectorl1distance:        {text: "vectorl1distance", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opvectorl1distanceimm:     {text: "vectorl1distanceimm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opvectorl2distance:        {text: "vectorl2distance", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opvectorl2distanceimm:     {text: "vectorl2distanceimm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opvectorcosinedistance:    {text: "vectorcosinedistance", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opvectorcosinedistanceimm: {text: "vectorcosinedistanceimm", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opCmpStrEqCs:              {text: "cmp_str_eq_cs", out: bcargs[3:4] /* {bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opCmpStrEqCi:              {text: "cmp_str_eq_ci", out: bcargs[3:4] /* {bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opCmpStrEqUTF8Ci:          {text: "cmp_str_eq_utf8_ci", out: bcargs[3:4] /* {bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opCmpStrFuzzyA3:           {text: "cmp_str_fuzzy_A3", out: bcargs[3:4] /* {bcK} */, in: bcargs[21:25] /* {bcS, bcS, bcDictSlot, bcK} */},
	opCmpStrFuzzyUnicodeA3:    {text: "cmp_str_fuzzy_unicode_A3", out: bcargs[3:4] /* {bcK} */, in: bcargs[21:25] /* {bcS, bcS, bcDictSlot, bcK} */},
	opHasSubstrFuzzyA3:        {text: "contains_fuzzy_A3", out: bcargs[3:4] /* {bcK} */, in: bcargs[21:25] /* {bcS, bcS, bcDictSlot, bcK} */},
	opHasSubstrFuzzyUnicodeA3: {text: "contains_fuzzy_unicode_A3", out: bcargs[3:4] /* {bcK} */, in: bcargs[21:25] /* {bcS, bcS, bcDictSlot, bcK} */},
	opSkip1charLeft:           {text: "skip_1char_left", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opSkip1charRight:          {text: "skip_1char_right", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opSkipNcharLeft:           {text: "skip_nchar_left", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opSkipNcharRight:          {text: "skip_nchar_right", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[1:4] /* {bcS, bcS, bcK} */},
	opTrimWsLeft:              {text: "trim_ws_left", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opTrimWsRight:             {text: "trim_ws_right", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opTrim4charLeft:           {text: "trim_char_left", out: bcargs[1:2] /* {bcS} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opTrim4charRight:          {text: "trim_char_right", out: bcargs[1:2] /* {bcS} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opoctetlength:             {text: "octetlength", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opcharlength:              {text: "characterlength", out: bcargs[1:2] /* {bcS} */, in: bcargs[2:4] /* {bcS, bcK} */},
	opSubstr:                  {text: "substr", out: bcargs[1:2] /* {bcS} */, in: bcargs[43:47] /* {bcS, bcS, bcS, bcK} */},
	opSplitPart:               {text: "split_part", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[47:51] /* {bcS, bcDictSlot, bcS, bcK} */},
	opContainsPrefixCs:        {text: "contains_prefix_cs", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opContainsPrefixCi:        {text: "contains_prefix_ci", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opContainsPrefixUTF8Ci:    {text: "contains_prefix_utf8_ci", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opContainsSuffixCs:        {text: "contains_suffix_cs", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opContainsSuffixCi:        {text: "contains_suffix_ci", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opContainsSuffixUTF8Ci:    {text: "contains_suffix_utf8_ci", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opContainsSubstrCs:        {text: "contains_substr_cs", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opContainsSubstrCi:        {text: "contains_substr_ci", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opContainsSubstrUTF8Ci:    {text: "contains_substr_utf8_ci", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opEqPatternCs:             {text: "eq_pattern_cs", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opEqPatternCi:             {text: "eq_pattern_ci", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opEqPatternUTF8Ci:         {text: "eq_pattern_utf8_ci", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opContainsPatternCs:       {text: "contains_pattern_cs", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opContainsPatternCi:       {text: "contains_pattern_ci", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opContainsPatternUTF8Ci:   {text: "contains_pattern_utf8_ci", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opIsSubnetOfIP4:           {text: "is_subnet_of_ip4", out: bcargs[3:4] /* {bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opDfaT6:                   {text: "dfa_tiny6", out: bcargs[3:4] /* {bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opDfaT7:                   {text: "dfa_tiny7", out: bcargs[3:4] /* {bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opDfaT8:                   {text: "dfa_tiny8", out: bcargs[3:4] /* {bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opDfaT6Z:                  {text: "dfa_tiny6Z", out: bcargs[3:4] /* {bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opDfaT7Z:                  {text: "dfa_tiny7Z", out: bcargs[3:4] /* {bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opDfaT8Z:                  {text: "dfa_tiny8Z", out: bcargs[3:4] /* {bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opDfaLZ:                   {text: "dfa_largeZ", out: bcargs[3:4] /* {bcK} */, in: bcargs[22:25] /* {bcS, bcDictSlot, bcK} */},
	opAggTDigest:              {text: "aggtdigest.f64", in: bcargs[85:88] /* {bcAggSlot, bcS, bcK} */},
	opslower:                  {text: "slower", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */, scratch: PageSize},
	opsupper:                  {text: "supper", out: bcargs[2:4] /* {bcS, bcK} */, in: bcargs[2:4] /* {bcS, bcK} */, scratch: PageSize},
	opaggapproxcount:          {text: "aggapproxcount", in: bcargs[29:33] /* {bcAggSlot, bcH, bcImmU16, bcK} */},
	opaggslotapproxcount:      {text: "aggslotapproxcount", in: bcargs[99:104] /* {bcAggSlot, bcL, bcH, bcImmU16, bcK} */},
	oppowuintf64:              {text: "powuint.f64", out: bcargs[1:2] /* {bcS} */, in: bcargs[26:29] /* {bcS, bcImmI64, bcK} */},
}

var bcargs = [104]bcArgType{bcImmI64, bcS, bcS, bcK, bcAggSlot, bcL, bcK,
	bcK, bcH, bcV, bcK, bcB, bcH, bcK, bcS, bcS, bcImmU16, bcK, bcS,
	bcImmF64, bcK, bcS, bcS, bcDictSlot, bcK, bcS, bcS, bcImmI64, bcK,
	bcAggSlot, bcH, bcImmU16, bcK, bcV, bcLitRef, bcK, bcAggSlot, bcK,
	bcK, bcK, bcImmU16, bcK, bcS, bcS, bcS, bcS, bcK, bcS, bcDictSlot,
	bcS, bcK, bcB, bcK, bcB, bcV, bcK, bcSymbolID, bcK, bcV, bcImmU16,
	bcK, bcAggSlot, bcL, bcS, bcK, bcAuxSlot, bcS, bcV, bcK, bcV, bcK,
	bcK, bcV, bcS, bcK, bcV, bcV, bcK, bcS, bcS, bcImmU64, bcK,
	bcSymbolID, bcV, bcK, bcAggSlot, bcS, bcK, bcS, bcK, bcV, bcImmF64,
	bcK, bcV, bcImmI64, bcK, bcB, bcSymbolID, bcK, bcAggSlot, bcL, bcH,
	bcImmU16, bcK}

const (
	optrap                    bcop = 0
	opbroadcasti64            bcop = 1
	opabsi64                  bcop = 2
	opnegi64                  bcop = 3
	opsigni64                 bcop = 4
	opsquarei64               bcop = 5
	opbitnoti64               bcop = 6
	opbitcounti64             bcop = 7
	opbitcounti64v2           bcop = 8
	opaddi64                  bcop = 9
	opaddi64imm               bcop = 10
	opsubi64                  bcop = 11
	opsubi64imm               bcop = 12
	oprsubi64imm              bcop = 13
	opmuli64                  bcop = 14
	opmuli64imm               bcop = 15
	opdivi64                  bcop = 16
	opdivi64imm               bcop = 17
	oprdivi64imm              bcop = 18
	opmodi64                  bcop = 19
	opmodi64imm               bcop = 20
	oprmodi64imm              bcop = 21
	oppmodi64                 bcop = 22
	oppmodi64imm              bcop = 23
	oprpmodi64imm             bcop = 24
	opaddmuli64imm            bcop = 25
	opminvaluei64             bcop = 26
	opminvaluei64imm          bcop = 27
	opmaxvaluei64             bcop = 28
	opmaxvaluei64imm          bcop = 29
	opandi64                  bcop = 30
	opandi64imm               bcop = 31
	opori64                   bcop = 32
	opori64imm                bcop = 33
	opxori64                  bcop = 34
	opxori64imm               bcop = 35
	opslli64                  bcop = 36
	opslli64imm               bcop = 37
	opsrai64                  bcop = 38
	opsrai64imm               bcop = 39
	opsrli64                  bcop = 40
	opsrli64imm               bcop = 41
	opbroadcastf64            bcop = 42
	opabsf64                  bcop = 43
	opnegf64                  bcop = 44
	opsignf64                 bcop = 45
	opsquaref64               bcop = 46
	oproundf64                bcop = 47
	oproundevenf64            bcop = 48
	optruncf64                bcop = 49
	opfloorf64                bcop = 50
	opceilf64                 bcop = 51
	opaddf64                  bcop = 52
	opaddf64imm               bcop = 53
	opsubf64                  bcop = 54
	opsubf64imm               bcop = 55
	oprsubf64imm              bcop = 56
	opmulf64                  bcop = 57
	opmulf64imm               bcop = 58
	opdivf64                  bcop = 59
	opdivf64imm               bcop = 60
	oprdivf64imm              bcop = 61
	opmodf64                  bcop = 62
	opmodf64imm               bcop = 63
	oprmodf64imm              bcop = 64
	oppmodf64                 bcop = 65
	oppmodf64imm              bcop = 66
	oprpmodf64imm             bcop = 67
	opminvaluef64             bcop = 68
	opminvaluef64imm          bcop = 69
	opmaxvaluef64             bcop = 70
	opmaxvaluef64imm          bcop = 71
	opsqrtf64                 bcop = 72
	opcbrtf64                 bcop = 73
	opexpf64                  bcop = 74
	opexp2f64                 bcop = 75
	opexp10f64                bcop = 76
	opexpm1f64                bcop = 77
	oplnf64                   bcop = 78
	opln1pf64                 bcop = 79
	oplog2f64                 bcop = 80
	oplog10f64                bcop = 81
	opsinf64                  bcop = 82
	opcosf64                  bcop = 83
	optanf64                  bcop = 84
	opasinf64                 bcop = 85
	opacosf64                 bcop = 86
	opatanf64                 bcop = 87
	opatan2f64                bcop = 88
	ophypotf64                bcop = 89
	oppowf64                  bcop = 90
	opret                     bcop = 91
	opretk                    bcop = 92
	opretbk                   bcop = 93
	opretsk                   bcop = 94
	opretbhk                  bcop = 95
	opinit                    bcop = 96
	opbroadcast0k             bcop = 97
	opbroadcast1k             bcop = 98
	opfalse                   bcop = 99
	opnotk                    bcop = 100
	opandk                    bcop = 101
	opandnk                   bcop = 102
	opork                     bcop = 103
	opxork                    bcop = 104
	opxnork                   bcop = 105
	opcvtktof64               bcop = 106
	opcvtktoi64               bcop = 107
	opcvti64tok               bcop = 108
	opcvtf64tok               bcop = 109
	opcvti64tof64             bcop = 110
	opcvttruncf64toi64        bcop = 111
	opcvtfloorf64toi64        bcop = 112
	opcvtceilf64toi64         bcop = 113
	opcvti64tostr             bcop = 114
	opcmpv                    bcop = 115
	opsortcmpvnf              bcop = 116
	opsortcmpvnl              bcop = 117
	opcmpvk                   bcop = 118
	opcmpvkimm                bcop = 119
	opcmpvi64                 bcop = 120
	opcmpvi64imm              bcop = 121
	opcmpvf64                 bcop = 122
	opcmpvf64imm              bcop = 123
	opcmpltstr                bcop = 124
	opcmplestr                bcop = 125
	opcmpgtstr                bcop = 126
	opcmpgestr                bcop = 127
	opcmpltk                  bcop = 128
	opcmpltkimm               bcop = 129
	opcmplek                  bcop = 130
	opcmplekimm               bcop = 131
	opcmpgtk                  bcop = 132
	opcmpgtkimm               bcop = 133
	opcmpgek                  bcop = 134
	opcmpgekimm               bcop = 135
	opcmpeqf64                bcop = 136
	opcmpeqf64imm             bcop = 137
	opcmpltf64                bcop = 138
	opcmpltf64imm             bcop = 139
	opcmplef64                bcop = 140
	opcmplef64imm             bcop = 141
	opcmpgtf64                bcop = 142
	opcmpgtf64imm             bcop = 143
	opcmpgef64                bcop = 144
	opcmpgef64imm             bcop = 145
	opcmpeqi64                bcop = 146
	opcmpeqi64imm             bcop = 147
	opcmplti64                bcop = 148
	opcmplti64imm             bcop = 149
	opcmplei64                bcop = 150
	opcmplei64imm             bcop = 151
	opcmpgti64                bcop = 152
	opcmpgti64imm             bcop = 153
	opcmpgei64                bcop = 154
	opcmpgei64imm             bcop = 155
	opisnanf                  bcop = 156
	opchecktag                bcop = 157
	optypebits                bcop = 158
	opisnullv                 bcop = 159
	opisnotnullv              bcop = 160
	opistruev                 bcop = 161
	opisfalsev                bcop = 162
	opcmpeqslice              bcop = 163
	opcmpeqv                  bcop = 164
	opcmpeqvimm               bcop = 165
	opdateaddmonth            bcop = 166
	opdateaddmonthimm         bcop = 167
	opdateaddyear             bcop = 168
	opdateaddquarter          bcop = 169
	opdatebin                 bcop = 170
	opdatediffmicrosecond     bcop = 171
	opdatediffparam           bcop = 172
	opdatediffmqy             bcop = 173
	opdateextractmicrosecond  bcop = 174
	opdateextractmillisecond  bcop = 175
	opdateextractsecond       bcop = 176
	opdateextractminute       bcop = 177
	opdateextracthour         bcop = 178
	opdateextractday          bcop = 179
	opdateextractdow          bcop = 180
	opdateextractdoy          bcop = 181
	opdateextractmonth        bcop = 182
	opdateextractquarter      bcop = 183
	opdateextractyear         bcop = 184
	opdatetounixepoch         bcop = 185
	opdatetounixmicro         bcop = 186
	opdatetruncmillisecond    bcop = 187
	opdatetruncsecond         bcop = 188
	opdatetruncminute         bcop = 189
	opdatetrunchour           bcop = 190
	opdatetruncday            bcop = 191
	opdatetruncdow            bcop = 192
	opdatetruncmonth          bcop = 193
	opdatetruncquarter        bcop = 194
	opdatetruncyear           bcop = 195
	opunboxts                 bcop = 196
	opboxts                   bcop = 197
	opwidthbucketf64          bcop = 198
	opwidthbucketi64          bcop = 199
	optimebucketts            bcop = 200
	opgeohash                 bcop = 201
	opgeohashimm              bcop = 202
	opgeotilex                bcop = 203
	opgeotiley                bcop = 204
	opgeotilees               bcop = 205
	opgeotileesimm            bcop = 206
	opgeodistance             bcop = 207
	opalloc                   bcop = 208
	opconcatstr               bcop = 209
	opfindsym                 bcop = 210
	opfindsym2                bcop = 211
	opblendv                  bcop = 212
	opblendf64                bcop = 213
	opunpack                  bcop = 214
	opunsymbolize             bcop = 215
	opunboxktoi64             bcop = 216
	opunboxcoercef64          bcop = 217
	opunboxcoercei64          bcop = 218
	opunboxcvtf64             bcop = 219
	opunboxcvti64             bcop = 220
	opboxf64                  bcop = 221
	opboxi64                  bcop = 222
	opboxk                    bcop = 223
	opboxstr                  bcop = 224
	opboxlist                 bcop = 225
	opmakelist                bcop = 226
	opmakestruct              bcop = 227
	ophashvalue               bcop = 228
	ophashvalueplus           bcop = 229
	ophashmember              bcop = 230
	ophashlookup              bcop = 231
	opaggandk                 bcop = 232
	opaggork                  bcop = 233
	opaggslotsumf             bcop = 234
	opaggsumf                 bcop = 235
	opaggsumi                 bcop = 236
	opaggminf                 bcop = 237
	opaggmini                 bcop = 238
	opaggmaxf                 bcop = 239
	opaggmaxi                 bcop = 240
	opaggandi                 bcop = 241
	opaggori                  bcop = 242
	opaggxori                 bcop = 243
	opaggcount                bcop = 244
	opaggmergestate           bcop = 245
	opaggbucket               bcop = 246
	opaggslotandk             bcop = 247
	opaggslotork              bcop = 248
	opaggslotsumi             bcop = 249
	opaggslotavgf             bcop = 250
	opaggslotavgi             bcop = 251
	opaggslotminf             bcop = 252
	opaggslotmini             bcop = 253
	opaggslotmaxf             bcop = 254
	opaggslotmaxi             bcop = 255
	opaggslotandi             bcop = 256
	opaggslotori              bcop = 257
	opaggslotxori             bcop = 258
	opaggslotcount            bcop = 259
	opaggslotcountv2          bcop = 260
	opaggslotmergestate       bcop = 261
	oplitref                  bcop = 262
	opauxval                  bcop = 263
	opsplit                   bcop = 264
	optuple                   bcop = 265
	opmovk                    bcop = 266
	opzerov                   bcop = 267
	opmovv                    bcop = 268
	opmovvk                   bcop = 269
	opmovf64                  bcop = 270
	opmovi64                  bcop = 271
	opobjectsize              bcop = 272
	oparraysize               bcop = 273
	oparrayposition           bcop = 274
	oparraysum                bcop = 275
	opvectorinnerproduct      bcop = 276
	opvectorinnerproductimm   bcop = 277
	opvectorl1distance        bcop = 278
	opvectorl1distanceimm     bcop = 279
	opvectorl2distance        bcop = 280
	opvectorl2distanceimm     bcop = 281
	opvectorcosinedistance    bcop = 282
	opvectorcosinedistanceimm bcop = 283
	opCmpStrEqCs              bcop = 284
	opCmpStrEqCi              bcop = 285
	opCmpStrEqUTF8Ci          bcop = 286
	opCmpStrFuzzyA3           bcop = 287
	opCmpStrFuzzyUnicodeA3    bcop = 288
	opHasSubstrFuzzyA3        bcop = 289
	opHasSubstrFuzzyUnicodeA3 bcop = 290
	opSkip1charLeft           bcop = 291
	opSkip1charRight          bcop = 292
	opSkipNcharLeft           bcop = 293
	opSkipNcharRight          bcop = 294
	opTrimWsLeft              bcop = 295
	opTrimWsRight             bcop = 296
	opTrim4charLeft           bcop = 297
	opTrim4charRight          bcop = 298
	opoctetlength             bcop = 299
	opcharlength              bcop = 300
	opSubstr                  bcop = 301
	opSplitPart               bcop = 302
	opContainsPrefixCs        bcop = 303
	opContainsPrefixCi        bcop = 304
	opContainsPrefixUTF8Ci    bcop = 305
	opContainsSuffixCs        bcop = 306
	opContainsSuffixCi        bcop = 307
	opContainsSuffixUTF8Ci    bcop = 308
	opContainsSubstrCs        bcop = 309
	opContainsSubstrCi        bcop = 310
	opContainsSubstrUTF8Ci    bcop = 311
	opEqPatternCs             bcop = 312
	opEqPatternCi             bcop = 313
	opEqPatternUTF8Ci         bcop = 314
	opContainsPatternCs       bcop = 315
	opContainsPatternCi       bcop = 316
	opContainsPatternUTF8Ci   bcop = 317
	opIsSubnetOfIP4           bcop = 318
	opDfaT6                   bcop = 319
	opDfaT7                   bcop = 320
	opDfaT8                   bcop = 321
	opDfaT6Z                  bcop = 322
	opDfaT7Z                  bcop = 323
	opDfaT8Z                  bcop = 324
	opDfaLZ                   bcop = 325
	opAggTDigest              bcop = 326
	opslower                  bcop = 327
	opsupper                  bcop = 328
	opaggapproxcount          bcop = 329
	opaggslotapproxcount      bcop = 330
	oppowuintf64              bcop = 331
	_maxbcop                       = 332
)

type opreplace struct{ from, to bcop }

var patchAVX512Level2 []opreplace = []opreplace{
	{from: opbitcounti64v2, to: opbitcounti64},
	{from: opaggslotcountv2, to: opaggslotcount},
}

// checksum: b14d1d5a711062c44a41c305c22ce2f2
