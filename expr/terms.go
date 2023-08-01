// Copyright (C) 2022 Sneller, Inc.
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//go:build none

package main

import (
	"bytes"
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/scanner"
	"unicode"

	"github.com/SnellerInc/sneller/rules"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

var (
	ofile  string
	ifile  string
	stdout io.Writer
)

func init() {
	flag.StringVar(&ofile, "o", "", "output file")
	flag.StringVar(&ifile, "i", "", "input file")
}

// error w/ positional information preserved
func fatalposf(pos *scanner.Position, f string, args ...any) {
	fatalf("%s: %s\n", pos, fmt.Sprintf(f, args...))
}

type env map[string]int

func trivial(t *rules.Term) bool {
	return (t.Name == "" || t.Name == "_") && t.Value == nil
}

func head(pos *scanner.Position, l rules.List) string {
	if len(l) == 0 {
		fatalposf(pos, "empty list (need atom in head position)")
	}
	if l[0].Value != nil {
		fatalposf(&l[0].Location, "head position of list not an atom")
	}
	return l[0].Name
}

func wrap(typename string, lit rules.String) string {
	return fmt.Sprintf("%s(%s)", typename, string(lit))
}

// input sets the input variable for a term
// and updates the input variables for any
// sub-terms that may be present
func input(t *rules.Term, invar string, depth, breadth int) {
	if t.Name == "" {
		t.Name = "_" // auto-assign to _
	}
	t.Input = invar
	if e, ok := t.Value.(rules.List); ok {
		op := head(&t.Location, e)
		c := classOf(op)

		// when we have (const x) we re-interpret
		// this as x:(const _)
		if t.Name == "_" && c == classConst {
			t.Name, e[1].Name = e[1].Name, t.Name
			if t.Name == "" {
				t.Name = "_"
			}
		}

		if t.Name == "_" && len(e) > 1 &&
			(c != classConst || e[1].Value != nil) {
			// will need a real identifier to
			// match the children
			t.Name = fmt.Sprintf("_tmp%03d%03d", depth, breadth)
		}
		args := e[1:]
		for i := range args {
			// a hack: when we have e.g. (int "1")
			// we can omit the explicit cast to Integer
			// by wrapping the argument with the *known* type
			if i == 0 && c == classConst {
				if lit, ok := args[i].Value.(rules.String); ok {
					args[i].Value = rules.String(wrap(c.auxtab[op], lit))
				}
			}
			input(&args[i], c.argnum(c, i, t.Name), depth+1, i)
		}
	}
}

// flatten performs a breadth-first flattening
// of all of the terms to be matched (typically
// you will want to do this only after having
// called t.input to associate input variables correctly)
func flattenTerm(t *rules.Term, dst []rules.Term) []rules.Term {
	if e, ok := t.Value.(rules.List); ok {
		args := e[1:]
		for i := range args {
			if !trivial(&args[i]) {
				dst = append(dst, args[i])
			}
		}
		for i := range args {
			dst = flattenTerm(&args[i], dst)
		}
	}
	return dst
}

// ifexpr constructs the inside of an 'if'
func ifexpr(t *rules.Term, bound env) string {
	if t.Value == nil {
		if t.Name == "_" {
			// we should have optimized these away
			panic("emitting trivial term...?")
		}
		var x string
		if bound[t.Name] > 0 {
			x = fmt.Sprintf("%s.Equals(%s)", t.Name, t.Input)
		} else {
			x = fmt.Sprintf("%s := %s; true", t.Name, t.Input)
		}
		bound[t.Name]++
		return x
	}
	switch p := t.Value.(type) {
	case rules.String:
		if t.Name != "_" {
			fatalposf(&t.Location, "unexpected binding of literal go string")
		}
		return fmt.Sprintf("%s.Equals(%s)", string(p), t.Input)
	case rules.List:
		if bound[t.Name] > 0 {
			fatalposf(&t.Location, "variable %s re-bound", t.Name)
		}
		op := head(&t.Location, p)
		c := classOf(op)
		if c == nil {
			fatalposf(&t.Location, "unrecognized op %s", op)
		}
		return c.match(c, op, p[1:], t.Name, t.Input)
	default:
		fatalposf(&t.Location, "bad pattern %T", p)
		return ""
	}
}

func splitFirst(r *rules.Rule) (string, []rules.Term) {
	conj := r.From
	if len(conj) == 0 {
		fatalf("rule with empty lhs")
	}
	pat := r.From[0]
	lst, ok := pat.(rules.List)
	if !ok {
		fatalposf(&r.Location, "expected first part of rule to be a list pattern")
	}
	return head(&r.Location, lst), lst[1:]
}

func inputRule(r *rules.Rule, invar string) {
	op, args := splitFirst(r)
	c := classOf(op)
	for i := range args {
		input(&args[i], c.argnum(c, i, invar), 0, i)
	}
}

func flattenRule(r *rules.Rule, dst []rules.Term) []rules.Term {
	_, args := splitFirst(r)
	dst = append(dst, args...)
	for i := range args {
		dst = flattenTerm(&args[i], dst)
	}
	return dst
}

// matchgen walks rules an emits the
// matching half of the rule
type matchgen struct {
	bound env
	depth int
}

// opclass contains the methods associated
// with a particular operand "class"
// (which corresponds 1:1 with a Go
// type implementing expr.Node)
type opclass struct {
	// typename is the concrete type
	// that implements expr.Node
	typename string

	// casematch should generate a switch
	// that matches all of the provided rules;
	// the rules are guaranteed to be associated
	// with this class
	casematch func(c *opclass, rules []rule)

	// argnum should produce the expression
	// that accesses the nth arg of an expression
	// given the outer binding bind
	argnum func(c *opclass, i int, bind string) string

	// match should generate a single match
	// for an expression given the match generator,
	// this opclass, the expression, the binding variable,
	// and the textual expression representing the value to unify
	match func(c *opclass, op string, args []rules.Term, bind, body string) string

	// cons should generate a constructor for this class
	// given the s-expression exp
	cons func(c *opclass, op string, args []rules.Term)

	// auxtab is used to store additional
	// information used by match and casematch
	auxtab map[string]string
}

var (
	classBuiltin          *opclass
	classComparison       *opclass
	classLogical          *opclass
	classBinaryArithmetic *opclass
	classIsKey            *opclass
	classConst            *opclass
	classConstNumber      *opclass
	classNull             *opclass
	classPatmatch         *opclass
	classLogicalNot       *opclass

	op2class    map[string]*opclass
	op2builtin  map[string]string
	builtinargs map[string][]string
)

func init() {
	classNull = &opclass{
		casematch: nil,
		match:     nullMatch,
		cons:      nullCons,
	}
	classConst = &opclass{
		casematch: nil,
		argnum:    constArgnum,
		match:     constMatch,
		cons:      constCons,
		auxtab: map[string]string{
			// name -> Go typename
			"rat":      "*Rational",
			"float":    "Float",
			"string":   "String",
			"ts":       "*Timestamp",
			"int":      "Integer",
			"constant": "Constant",
			"bool":     "Bool",
			"struct":   "*Struct",
			"list":     "*List",
		},
	}
	classConstNumber = &opclass{
		casematch: nil,
		argnum:    constArgnum,
		match:     constNumericMatch,
		cons:      constCons,
		auxtab: map[string]string{
			// name -> Go typename
			"number": "number",
		},
	}
	classBuiltin = &opclass{
		typename:  "*Builtin",
		casematch: builtinCasematch,
		argnum:    builtinArgnum,
		match:     builtinMatch,
		cons:      builtinCons,
	}
	classComparison = &opclass{
		typename:  "*Comparison",
		argnum:    binaryArgnum,
		casematch: binaryCasematch,
		match:     binaryMatch,
		cons:      binaryCons,
		auxtab: map[string]string{
			"eq":  "Equals",
			"neq": "NotEquals",
			"lt":  "Less",
			"lte": "LessEquals",
			"gt":  "Greater",
			"gte": "GreaterEquals",
		},
	}
	classPatmatch = &opclass{
		typename:  "*StringMatch",
		argnum:    patternArgnum,
		casematch: binaryCasematch,
		match:     binaryMatch,
		cons:      patternCons,
		auxtab: map[string]string{
			"like":  "Like",
			"ilike": "Ilike",
			"rx":    "RegexpMatch",
			"rxci":  "RegexpMatchCi",
		},
	}
	classLogical = &opclass{
		typename:  "*Logical",
		argnum:    binaryArgnum,
		casematch: binaryCasematch,
		match:     binaryMatch,
		cons:      binaryCons,
		auxtab: map[string]string{
			"and":  "OpAnd",
			"or":   "OpOr",
			"xnor": "OpXnor",
			"xor":  "OpXor",
		},
	}
	classBinaryArithmetic = &opclass{
		typename:  "*Arithmetic",
		argnum:    binaryArgnum,
		casematch: binaryCasematch,
		match:     binaryMatch,
		cons:      binaryCons,
		auxtab: map[string]string{
			"add": "AddOp",
			"sub": "SubOp",
			"mul": "MulOp",
			"div": "DivOp",
			"mod": "ModOp",
		},
	}
	classIsKey = &opclass{
		typename:  "*IsKey",
		argnum:    unaryArgnum,
		casematch: iskeyCasematch,
		match:     iskeyMatch,
		cons:      isKeyCons,
	}
	classLogicalNot = &opclass{
		typename: "Not",
		argnum:   unaryArgnum,
		cons:     logicalNotCons,
	}

	// for s-expressions, we assume that
	// the head of the list is a builtin op
	// *unless* it matches one of these identifiers:
	op2class = map[string]*opclass{
		"and":            classLogical,
		"or":             classLogical,
		"xor":            classLogical,
		"xnor":           classLogical,
		"eq":             classComparison,
		"neq":            classComparison,
		"lt":             classComparison,
		"lte":            classComparison,
		"gt":             classComparison,
		"gte":            classComparison,
		"like":           classPatmatch,
		"ilike":          classPatmatch,
		"rx":             classPatmatch,
		"rxci":           classPatmatch,
		"add":            classBinaryArithmetic,
		"sub":            classBinaryArithmetic,
		"mul":            classBinaryArithmetic,
		"div":            classBinaryArithmetic,
		"mod":            classBinaryArithmetic,
		"sra":            classBinaryArithmetic,
		"srl":            classBinaryArithmetic,
		"sll":            classBinaryArithmetic,
		"is_null":        classIsKey,
		"is_not_null":    classIsKey,
		"is_missing":     classIsKey,
		"is_not_missing": classIsKey,
		"is_true":        classIsKey,
		"is_not_true":    classIsKey,
		"is_false":       classIsKey,
		"is_not_false":   classIsKey,
		"not":            classLogicalNot,

		"string":   classConst,
		"bool":     classConst,
		"float":    classConst,
		"int":      classConst,
		"constant": classConst,
		"number":   classConstNumber,
		"ts":       classConst,
		"list":     classConst,
		"struct":   classConst,

		"null":    classNull,
		"missing": classNull,
	}

	// name => BuiltinOp (only non-trivial renames)
	op2builtin = map[string]string{
		"contains_ci":  "ContainsCI",
		"equals_ci":    "EqualsCI",
		"assert_str":   "AssertIonType",
		"assert_int":   "AssertIonType",
		"assert_float": "AssertIonType",
		"assert_num":   "AssertIonType",
		"pow-uint":     "PowUint",
	}

	builtinargs = map[string][]string{
		"assert_str":   []string{"Integer(0x8)"},
		"assert_int":   []string{"Integer(0x2)", "Integer(0x3)"},
		"assert_float": []string{"Integer(0x4)"},
		"assert_num":   []string{"Integer(0x2)", "Integer(0x3)", "Integer(0x4)"},
	}
}

func classOf(op string) *opclass {
	if c, ok := op2class[op]; ok {
		return c
	}
	return classBuiltin
}

func fatalf(f string, args ...any) {
	fmt.Fprintf(os.Stderr, f, args...)
	os.Exit(1)
}

func binaryArgnum(c *opclass, i int, bind string) string {
	switch i {
	case 0:
		return bind + ".Left"
	case 1:
		return bind + ".Right"
	default:
		fatalf("invalid argument %d for binary type", i)
		return ""
	}
}

func binaryMatch(c *opclass, op string, args []rules.Term, bind, expvar string) string {
	if len(args) != 2 {
		fatalf("have %d args for %s; want 2", len(args), op)
	}
	opname, ok := c.auxtab[op]
	if !ok {
		fatalf("unrecognized op %s", op)
	}
	return fmt.Sprintf("%s, ok := (%s).(%s); ok && %s.Op == %s",
		bind, expvar, c.typename, bind, opname)
}

func patternArgnum(c *opclass, i int, bind string) string {
	switch i {
	case 0:
		return bind + ".Expr"
	case 1:
		return bind + ".Pattern"
	case 2:
		return bind + ".Escape"
	default:
		fatalf("invalid argument %d for pattern-match type", i)
		return ""
	}
}

func patternCons(c *opclass, op string, args []rules.Term) {
	if len(args) != 2 && len(args) != 3 {
		fatalposf(&args[0].Location, "op %s needs two or three arguments\n", op)
	}
	op, ok := c.auxtab[op]
	if !ok {
		fatalf("op %s unknown", op)
	}
	fmt.Fprintf(stdout, "&%s{Op: %s, Expr: ", c.typename[1:], op)
	emitCons(&args[0])
	fmt.Fprintf(stdout, ", Pattern: ")
	emitCons(&args[1])
	if len(args) == 3 {
		fmt.Fprintf(stdout, ", Escape: ")
		emitCons(&args[2])
	}
	fmt.Fprintf(stdout, "}")
}

func unaryArgnum(c *opclass, i int, bind string) string {
	if i != 0 {
		fatalf("argument index %d invalid for unary ops", i)
	}
	return bind + ".Expr"
}

func iskeyMatch(c *opclass, op string, args []rules.Term, bind, expvar string) string {
	if len(args) != 1 {
		fatalf("have %d args for %s", len(args), op)
	}
	key := snake2Pascal(op) // is_null -> IsNull
	return fmt.Sprintf("%s, ok := (%s).(*IsKey); ok && %s.Key == %s",
		bind, expvar, bind, key)
}

func builtinArgnum(c *opclass, i int, bind string) string {
	return fmt.Sprintf("%s.Args[%d]", bind, i)
}

func builtinMatch(c *opclass, op string, args []rules.Term, bind, expvar string) string {
	return fmt.Sprintf("%s, ok := (%s).(*Builtin); ok && %s.Func == %s && len(%s.Args) == %d",
		bind, expvar, bind, builtinName(op), bind, len(args))
}

func constArgnum(c *opclass, i int, bind string) string {
	return bind
}

func castBind(bind, expvar, gotype string) string {
	return fmt.Sprintf("%s, ok := (%s).(%s); ok", bind, expvar, gotype)
}

func constMatch(c *opclass, op string, args []rules.Term, bind, expvar string) string {
	if len(args) != 1 {
		fatalf("expected const op %s to have 1 arg", op)
	}
	gotype, ok := c.auxtab[op]
	if !ok {
		fatalf("unrecognized const type %s", op)
	}
	// if we are matching a go literal constant,
	// it needs to be cast appropriately
	if lit, ok := args[0].Value.(rules.String); ok {
		args[0].Value = rules.String(wrap(gotype, lit))
	}
	return castBind(bind, expvar, gotype)
}

func constNumericMatch(c *opclass, op string, args []rules.Term, bind, expvar string) string {
	if len(args) != 1 {
		fatalf("expected const op %s to have 1 arg", op)
	}
	gotype, ok := c.auxtab[op]
	if !ok {
		fatalf("unrecognized const type %s", op)
	}
	// if we are matching a go literal constant,
	// it needs to be cast appropriately
	if lit, ok := args[0].Value.(rules.String); ok {
		args[0].Value = rules.String(wrap(gotype, lit))
	}
	return fmt.Sprintf("%s := asrational(%s); %[1]s != nil", bind, expvar)
}

func nullMatch(c *opclass, op string, args []rules.Term, bind, expvar string) string {
	if len(args) != 0 {
		fatalf("expected null op %s to have 1 arg", op)
	}
	return castBind(bind, expvar, snake2Pascal(op))
}

func (m *matchgen) emitTerm(t *rules.Term, bound env) {
	fmt.Fprintf(stdout, "if %s {\n", ifexpr(t, bound))
	m.depth++
}

func (m *matchgen) emit(terms []rules.Term) {
	if m.bound == nil {
		m.bound = make(map[string]int)
	}
	for i := range terms {
		if !trivial(&terms[i]) {
			m.emitTerm(&terms[i], m.bound)
		}
	}
}

func (m *matchgen) emitPredicates(pos *scanner.Position, lst []rules.Value) {
	for i := range lst {
		str, ok := lst[i].(rules.String)
		if !ok {
			fatalf("expected literal string as conjunction pattern")
		}
		fmt.Fprintf(stdout, "if %s {\n", string(str))
		m.depth++
	}
}

func (m *matchgen) close() {
	for m.depth > 0 {
		fmt.Fprintf(stdout, "}\n")
		m.depth--
	}
}

func emitCons(t *rules.Term) {
	if t.Name != "_" && t.Name != "" {
		if t.Value != nil {
			fatalposf(&t.Location, "unexpected result")
		}
		fmt.Fprint(stdout, t.Name)
		return
	}
	switch p := t.Value.(type) {
	case rules.String:
		fmt.Fprint(stdout, string(p))
	case rules.List:
		emitConsExp(t, p)
	default:
		fatalposf(&t.Location, "cannot emit constructor for %T %v", p, p)
	}
}

func binaryCons(c *opclass, op string, args []rules.Term) {
	if len(args) != 2 {
		fatalf("op %s needs two arguments", op)
	}
	op, ok := c.auxtab[op]
	if !ok {
		fatalf("op %s unknown", op)
	}
	fmt.Fprintf(stdout, "&%s{Op: %s, Left: ", c.typename[1:], op)
	emitCons(&args[0])
	fmt.Fprintf(stdout, ", Right: ")
	emitCons(&args[1])
	fmt.Fprintf(stdout, "}")
}

func isKeyCons(c *opclass, op string, args []rules.Term) {
	fmt.Fprintf(stdout, "&IsKey{Key: %s, Expr:", snake2Pascal(op))
	emitCons(&args[0])
	fmt.Fprintf(stdout, "}")
}

func logicalNotCons(c *opclass, op string, args []rules.Term) {
	if op != "not" {
		panic("constructor only for not")
	}
	fmt.Fprintf(stdout, "&Not{Expr: ")
	emitCons(&args[0])
	fmt.Fprintf(stdout, "}")
}

func builtinCons(c *opclass, op string, args []rules.Term) {
	fmt.Fprintf(stdout, "Call(%s", builtinName(op))
	for i := range args {
		fmt.Fprintf(stdout, ", ")
		emitCons(&args[i])
	}
	{
		args, ok := builtinargs[op]
		if ok {
			for _, s := range args {
				fmt.Fprintf(stdout, ", %s", s)
			}
		}
	}
	fmt.Fprintf(stdout, ")")
}

func constCons(c *opclass, op string, args []rules.Term) {
	fmt.Fprintf(stdout, "%s(", c.auxtab[op])
	emitCons(&args[0])
	fmt.Fprintf(stdout, ")")
}

func nullCons(c *opclass, op string, args []rules.Term) {
	fmt.Fprintf(stdout, "%s{}", snake2Pascal(op))
}

func emitConsExp(t *rules.Term, lst []rules.Term) {
	op := head(&t.Location, lst)
	c := classOf(op)
	c.cons(c, op, lst[1:])
}

type rule struct {
	rules.Rule

	// decomposed head+rest
	op   string
	args []rules.Term
}

// group rules by name, then by argument count
func orderRules(rules []rule) {
	slices.SortStableFunc(rules, func(x, y rule) int {
		if x.op == y.op {
			return len(x.args) - len(y.args)
		}
		return strings.Compare(x.op, y.op)
	})
}

func emitArgs(r *rule, toplvl string, scratch []rules.Term) []rules.Term {
	fmt.Fprintf(stdout, "// %s\n", r.Rule.String())
	src := &rules.Term{
		Name:  toplvl,
		Value: r.Rule.From[0],
	}
	input(src, toplvl, 0, 0)
	scratch = flattenRule(&r.Rule, scratch[:0])
	m := matchgen{}
	m.emit(scratch)
	m.emitPredicates(&r.Location, r.From[1:])
	fmt.Fprintf(stdout, "return ")
	emitCons(&r.To)
	fmt.Fprintf(stdout, "\n")
	m.close()
	return scratch[:0]
}

// match binary terms in a switch
func binaryCasematch(c *opclass, in []rule) {
	if len(in) == 0 {
		return
	}
	orderRules(in)
	current := ""
	fmt.Fprintf(stdout, "switch src.Op {\n")

	var flat []rules.Term
	for i := range in {
		r := &in[i]
		opname, ok := c.auxtab[r.op]
		if !ok {
			fatalf("unknown op %s", r.op)
		}
		if current != opname {
			fmt.Fprintf(stdout, "case %s:\n", opname)
		}
		current = opname
		flat = emitArgs(r, "src", flat[:0])
	}
	fmt.Fprintf(stdout, "}\n")
}

func iskeyCasematch(c *opclass, in []rule) {
	if len(in) == 0 {
		return
	}
	fmt.Fprintf(stdout, "\tswitch src.Key {\n")
	orderRules(in)
	current := ""
	var flat []rules.Term
	for i := range in {
		r := &in[i]
		if len(r.args) != 1 {
			fatalf("have %d args for %s", len(r.args), r.op)
		}
		goname := snake2Pascal(r.op)
		if current != goname {
			fmt.Fprintf(stdout, "\tcase %s:\n", goname)
		}
		current = goname
		flat = emitArgs(r, "src", flat[:0])
	}
	fmt.Fprintf(stdout, "}\n") // close *IsKey.Key switch
}

func builtinName(op string) string {
	name, ok := op2builtin[op]
	if ok {
		return name
	}

	return snake2Pascal(op)
}

func builtinCasematch(c *opclass, in []rule) {
	if len(in) == 0 {
		return
	}
	orderRules(in)
	fmt.Fprintf(stdout, "\tswitch src.Func {\n")
	argc := -1
	current := ""
	var flat []rules.Term
	for i := range in {
		r := &in[i]
		name := builtinName(r.op)
		if current != name {
			if argc != -1 {
				fmt.Fprintf(stdout, "}\n") // close argcount check
			}
			fmt.Fprintf(stdout, "\tcase %s:\n", name)
			current = name
			argc = -1
		}

		// emit groupings by argument count
		if argc == -1 {
			fmt.Fprintf(stdout, "if len(src.Args) == %d {\n", len(r.args))
			argc = len(r.args)
		} else if len(r.args) > argc {
			fmt.Fprintf(stdout, "}\n")
			fmt.Fprintf(stdout, "if len(src.Args) == %d {\n", len(r.args))
			argc = len(r.args)
		}
		flat = emitArgs(r, "src", flat[:0])
	}
	if argc >= 0 {
		fmt.Fprintf(stdout, "}\n") // close argcount check
	}
	fmt.Fprintf(stdout, "}\n") // close src.Func switch
}

func constCasematch(c *opclass, rules []rule) {
	if len(rules) == 0 {
		return
	}
	// we don't expect this at the top level
	fatalf("unexpected constCasematch")
}

func writeRules(rules []rule) {
	fmt.Fprintf(stdout, "package expr\n\n")
	fmt.Fprintf(stdout, "// code generated by terms.go; DO NOT EDIT\n")
	fmt.Fprintf(stdout, "import \"strings\"\n")
	fmt.Fprintf(stdout, "import \"math/big\"\n")
	fmt.Fprintf(stdout, "import \"unicode/utf8\"\n")

	// collect class->rule relationship
	class2rules := make(map[*opclass][]rule)
	for i := range rules {
		c := classOf(rules[i].op)
		if c == nil {
			fatalf("op %s has no opclass?", rules[i].op)
		}
		class2rules[c] = append(class2rules[c], rules[i])
	}
	classes := maps.Keys(class2rules)
	slices.SortFunc(classes, func(x, y *opclass) int {
		return strings.Compare(x.typename, y.typename)
	})

	// emit a function for each distinct class
	for i, class := range classes {
		if class.typename == "" {
			fatalf("cannot use top-level pattern of type %T", class)
		}
		fmt.Fprintf(stdout, "\nfunc simplifyClass%d(src %s, h Hint) Node {\n", i, class.typename)
		class.casematch(class, class2rules[class])
		fmt.Fprintf(stdout, "return nil\n")
		fmt.Fprintf(stdout, "}\n")
	}

	fmt.Fprintf(stdout, "func simplify1(src Node, h Hint) Node {\n")
	// for each class, emit a case in the type switch
	// and then have the top-level rules in each class
	// match using a switch of their own
	fmt.Fprintf(stdout, "switch src := src.(type) {\n")
	for i, class := range classes {
		fmt.Fprintf(stdout, "\t case %s:\n", class.typename)
		fmt.Fprintf(stdout, "\t return simplifyClass%d(src, h)\n", i)
	}
	fmt.Fprintf(stdout, "}\n") // close switch

	fmt.Fprintf(stdout, "return nil\n") // default return
	fmt.Fprintf(stdout, "}\n")          // close func
}

// convert e.g. DATE_TRUNC -> DateTrunc
func snake2Pascal(str string) string {
	var out []rune
	cap := true
	for _, c := range str {
		if c == '_' {
			cap = true
			continue
		}
		if cap {
			c = unicode.ToUpper(c)
			cap = false
		} else {
			c = unicode.ToLower(c)
		}
		out = append(out, c)
	}
	return string(out)
}

func generate(path string) {
	f, err := os.Open(path)
	checkErr(err)
	defer f.Close()

	lst, err := rules.Parse(f)
	checkErr(err)

	var all []rule
	for i := range lst {
		op, args := splitFirst(&lst[i])
		all = append(all, rule{
			Rule: lst[i],
			op:   op,
			args: args,
		})
	}

	writeRules(all)
}

func main() {
	flag.Parse()
	buf := bytes.NewBuffer(nil)
	stdout = buf
	generate(ifile)

	checksum := []byte(fmt.Sprintf("// checksum: %x\n", md5.Sum(buf.Bytes())))
	regenerate := true
	old, err := os.ReadFile(ofile)
	if err == nil {
		regenerate = !bytes.HasSuffix(old, checksum)
	}

	if regenerate {
		fmt.Printf("Creating %q\n", ofile)

		f, err := os.Create(ofile)
		checkErr(err)
		defer f.Close()
		_, err = f.Write(buf.Bytes())
		checkErr(err)
		_, err = f.Write(checksum)
		checkErr(err)
	}
}

func checkErr(err error) {
	if err != nil {
		fatalf("%s\n", err)
	}
}
