//go:build genrewrite

package vm

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/SnellerInc/sneller/rules"

	"golang.org/x/exp/slices"
)

var stdout io.Writer

var name2op map[string]ssaop

func init() {
	name2op = make(map[string]ssaop, _ssamax)
	for i := 0; i < int(_ssamax); i++ {
		name2op[_ssainfo[i].text] = ssaop(i)
	}
}

var opname func(op ssaop) string

func fatalposf(pos *scanner.Position, f string, args ...any) {
	fatalf("%s: %s\n", pos, fmt.Sprintf(f, args...))
}

func fatalf(f string, args ...any) {
	fmt.Fprintf(os.Stderr, f, args...)
	os.Exit(1)
}

func splitFirst(r *rules.Rule) (ssaop, []rules.Term) {
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

func head(pos *scanner.Position, l rules.List) ssaop {
	if len(l) == 0 {
		fatalposf(pos, "empty list (need atom in head position)")
	}
	if l[0].Value != nil {
		fatalposf(&l[0].Location, "head position of list not an atom")
	}
	id, ok := name2op[l[0].Name]
	if !ok {
		fatalposf(&l[0].Location, "op %s doesn't exist", l[0].Name)
	}
	return id
}

func repeat(prefix string, suffixes ...string) []string {
	out := make([]string, len(suffixes))
	for i, suff := range suffixes {
		out[i] = prefix + suff
	}
	return out
}

type rule struct {
	rules.Rule

	op   ssaop
	args []rules.Term
}

var tempvar int

func gentmp() string {
	s := fmt.Sprintf("_tmp%d", tempvar)
	tempvar++
	return s
}

func inputTerm(t *rules.Term, invar string) {
	t.Input = invar
	if lst, ok := t.Value.(rules.List); ok {
		// FIXME: head here will complain about
		// the use of regexes in head positions
		// outside of the top-level forms;
		// in order to support them we need
		// to unify the argument layout of
		// each of the possible resolved forms...
		op := head(&t.Location, lst)
		args := lst[1:]
		var imm *rules.Term
		f := immfmt(0)
		if f = ssainfo[op].immfmt; f != 0 {
			imm = &args[len(args)-1]
			args = args[:len(args)-1]
		}
		// check argcount
		if len(args) != len(ssainfo[op].argtypes) {
			if len(ssainfo[op].vaArgs) == 0 {
				fatalposf(&t.Location, "op %s has %d arguments", op, len(ssainfo[op].argtypes))
			}
			argc := len(args) - len(ssainfo[op].argtypes)
			if argc%len(ssainfo[op].vaArgs) != 0 {
				fatalposf(&t.Location, "op %s must have a multiple of %d variadic args", op, len(ssainfo[op].vaArgs))
			}
		}
		// no explicit name, but need an implicit name
		// in order for the inner block to refer to this variable
		if (len(args) > 0 || imm != nil) && (t.Name == "" || t.Name == "_") {
			t.Name = gentmp()
		}
		for i := range args {
			inputTerm(&args[i], fmt.Sprintf("%s.args[%d]", t.Name, i))
		}
		if imm != nil && !trivial(imm) {
			inputTerm(imm, getimm(t.Name, f, imm))
		}
	}
}

func trivial(t *rules.Term) bool {
	return (t.Name == "" || t.Name == "_") && t.Value == nil
}

// create a breadth-first ordering of rules to evaluate
func flattenRule(r *rules.Rule, dst []rules.Term) []rules.Term {
	_, args := splitFirst(r)
	dst = append(dst, args...)
	for i := range args {
		dst = flattenTerm(&args[i], dst)
	}
	return dst
}

// expand top-level rules so that rules that
// begin with a regex as the head element are
// expanded into N rules for each matching opcode
func expandRules(lst []rules.Rule) []rules.Rule {
	var out []rules.Rule
	for i := range lst {
		rl, ok := lst[i].From[0].(rules.List)
		if !ok || len(rl) == 0 {
			fatalposf(&lst[i].Location, "expected first rule value to be a non-empty list")
		}
		head := rl[0]
		if head.Value == nil {
			// raw identifier
			out = append(out, lst[i])
			continue
		}
		str, ok := head.Value.(rules.String)
		if !ok {
			fatalposf(&head.Location, "unexpected list head (expected identifier or string)")
		}
		re, err := regexp.Compile(string(str))
		if err != nil {
			fatalposf(&head.Location, "bad regexp: %s", err)
		}
		// create a new top-level rule for each
		// matching regex
		matches := 0
		for opnum := 0; opnum < int(_ssamax); opnum++ {
			op := ssainfo[opnum].text
			if re.MatchString(op) {
				rc := lst[i]
				rc.From = slices.Clone(rc.From)
				arglst := slices.Clone(rl)
				arglst[0].Name = op
				arglst[0].Value = nil
				rc.From[0] = arglst
				out = append(out, rc)
				matches++
			}
		}
		if matches == 0 {
			fatalposf(&lst[i].Location, "regexp %q matches 0 ops", str)
		}
	}
	return out
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

func rxmatch(loc *scanner.Position, str rules.String) []ssaop {
	re, err := regexp.Compile(string(str))
	if err != nil {
		fatalposf(loc, "bad regexp: %s", err)
	}
	var matchops []ssaop
	for opnum := 0; opnum < int(_ssamax); opnum++ {
		op := ssainfo[opnum].text
		if re.MatchString(op) {
			matchops = append(matchops, ssaop(opnum))
		}
	}
	return matchops
}

func listmatch(lst []rules.Term, name, input string) string {
	head := lst[0]
	if head.Value == nil {
		id, ok := name2op[lst[0].Name]
		if !ok {
			fatalposf(&lst[0].Location, "op %s doesn't exist", lst[0].Name)
		}
		return match(id, len(lst)-1, name, input)
	}
	str, ok := head.Value.(rules.String)
	if !ok {
		fatalposf(&lst[0].Location, "unexpected head value")
	}
	ops := rxmatch(&head.Location, str)
	if len(ops) == 0 {
		fatalposf(&head.Location, "regexp %q matches 0 ops", str)
	}
	return matchany(ops, len(lst)-1, name, input)
}

func matchany(oplst []ssaop, argc int, name, input string) string {
	valen := -1
	arglen := -1
	var str strings.Builder
	// perhaps generating a LUT or something
	// would be faster here?
	fmt.Fprintf(&str, "%s := %s; (", name, input)
	for i, op := range oplst {
		if i == 0 {
			valen = len(ssainfo[op].vaArgs)
			arglen = len(ssainfo[op].argtypes)
		} else {
			if valen != len(ssainfo[op].vaArgs) {
				fatalf("incompatible ops")
			}
			if arglen != len(ssainfo[op].argtypes) {
				fatalf("incompatible ops")
			}
			str.WriteString(" || ")
		}
		fmt.Fprintf(&str, "%s.op == %s", name, opname(op))
	}
	// only bother matching argcount for variadic ops:
	if valen > 0 {
		fmt.Fprintf(&str, ") && len(%s.args == %d)", name, argc)
		return str.String()
	}
	str.WriteString(")")
	return str.String()
}

func match(op ssaop, argc int, name, input string) string {
	valen := len(ssainfo[op].vaArgs)
	ret := fmt.Sprintf("%s := %s; %s.op == %s", name, input, name, opname(op))
	if valen > 0 {
		ret += fmt.Sprintf(" && len(%s.args) == %d", name, argc)
	}
	return ret
}

func getimm(name string, f immfmt, term *rules.Term) string {
	switch f {
	case fmtdict:
		return fmt.Sprintf("%s.imm.(string)", name)
	case fmti64:
		return fmt.Sprintf("toi64(%s.imm)", name)
	case fmtf64:
		return fmt.Sprintf("tof64(%s.imm)", name)
	case fmtbool:
		return fmt.Sprintf("(toi64(%s.imm) != 0)", name)
	case fmtaggslot:
		return fmt.Sprintf("%s.imm.(aggregateslot)", name)
	case fmtslot:
		return fmt.Sprintf("%s.imm.(int)", name)
	case fmtother:
		return fmt.Sprintf("%s.imm", name)
	default:
		fatalposf(&term.Location, "no support for immfmt %d", f)
		return ""
	}
}

func ifexpr(t *rules.Term, bound map[string]int) string {
	if t.Value == nil {
		if t.Name == "_" {
			panic("emitting trivial term")
		}
		var x string
		if bound[t.Name] > 0 {
			x = fmt.Sprintf("%s == %s", t.Name, t.Input)
		} else {
			x = fmt.Sprintf("%s := %s; true", t.Name, t.Input)
		}
		bound[t.Name]++
		return x
	}
	// gross:
	imm := strings.Contains(t.Input, "imm")
	switch p := t.Value.(type) {
	case rules.Int:
		if !imm {
			fatalposf(&t.Location, "matching float against non-immediate")
		}
		return fmt.Sprintf("%s == %d", t.Input, int64(p))
	case rules.Float:
		if !imm {
			fatalposf(&t.Location, "matching integer against non-immediate")
		}
		return fmt.Sprintf("%s == %g", t.Input, float64(p))
	case rules.String:
		if t.Name != "_" && t.Name != "" {
			fatalposf(&t.Location, "unexpected binding of literal go string to %q", t.Name)
		}
		if imm {
			return fmt.Sprintf("%s == %q", t.Input, string(p))
		}
		return fmt.Sprintf("%s == %s", string(p), t.Input)
	case rules.List:
		if t.Name == "" {
			t.Name = gentmp()
		}
		if bound[t.Name] > 0 {
			fatalposf(&t.Location, "variable %s re-bound", t.Name)
		}
		bound[t.Name]++
		return listmatch(p, t.Name, t.Input)
	default:
		fatalposf(&t.Location, "bad pattern %T", p)
	}
	return ""
}

// group rules by name, then by argument count
func orderRules(rules []rule) {
	slices.SortFunc(rules, func(x, y rule) bool {
		if x.op == y.op {
			return len(x.args) < len(y.args)
		}
		return x.op < y.op
	})
}

func inputRule(r *rule, in string) {
	inputTerm(&rules.Term{Value: r.Rule.From[0], Name: in}, in)
}

func emit(lst []rules.Term) int {
	bound := make(map[string]int)
	n := 0
	for i := range lst {
		if trivial(&lst[i]) {
			continue
		}
		fmt.Fprintf(stdout, "if %s {\n", ifexpr(&lst[i], bound))
		n++
	}
	return n
}

func emitPredicates(lst []rules.Value) int {
	for i := range lst {
		str, ok := lst[i].(rules.String)
		if !ok {
			fatalf("unexpected predicate %s", lst[i])
		}
		fmt.Fprintf(stdout, "if %s {\n", string(str))
	}
	return len(lst)
}

func consExpr(t *rules.Term, toplvl bool, imm bool) string {
	if t.Value == nil {
		// matching a bound variable
		if t.Name == "" || t.Name == " " {
			fatalposf(&t.Location, "cannot produce an empty result")
		}
		return t.Name
	}
	switch p := t.Value.(type) {
	case rules.Float:
		if imm {
			return p.String()
		}
		fatalposf(&t.Location, "float doesn't match non-immediate")
	case rules.Int:
		if imm {
			return p.String()
		}
		fatalposf(&t.Location, "int doesn't match non-immediate")
	case rules.String:
		if imm {
			return strconv.Quote(string(p))
		}
		return string(p)
	case rules.List:
		op := head(&t.Location, p)
		args := p[1:]
		if op == sinit {
			return "p.values[0]" // init is always op 0 by convention
		}
		imm := ssainfo[op].immfmt != 0
		if imm {
			// ssaimm() expects the immediate first
			args = slices.Clone(args)
			last := args[len(args)-1]
			copy(args[1:], args)
			args[0] = last
		}
		var start string
		if imm {
			if toplvl {
				start = fmt.Sprintf("/* clobber v */ p.setssa(v, %s", opname(op))
			} else {
				start = fmt.Sprintf("p.ssaimm(%s", opname(op))
			}
		} else {
			if toplvl {
				start = fmt.Sprintf("/* clobber v */ p.setssa(v, %s, nil", opname(op))
			} else {
				start = fmt.Sprintf("p.ssaimm(%s, nil", opname(op))
			}
		}
		for i := range args {
			start += ", " + consExpr(&args[i], false, imm && i == 0)
		}
		start += ")"
		return start
	default:
		fatalf("unexpected rhs expression %s", t)
	}
	return ""
}

func emitCons(out *rules.Term) {
	fmt.Fprintf(stdout, "return %s, true\n", consExpr(out, true, false))
}

func casematch(lst []rule, vname string) {
	fmt.Fprintf(stdout, "switch %s.op {\n", vname)
	var scratch []rules.Term
	cur := ssaop(-1)
	narg := -1
	for i := range lst {
		if cur != lst[i].op {
			if narg != -1 {
				fmt.Fprintf(stdout, "}\n")
			}
			narg = -1
			cur = lst[i].op
			fmt.Fprintf(stdout, "case %s: /* %s */\n", opname(cur), cur)
		}
		argc := len(lst[i].args)
		if ssainfo[cur].immfmt != 0 {
			argc--
		}
		if argc != narg {
			if narg != -1 {
				fmt.Fprintf(stdout, "}\n") // close previous argument-match block
			}
			narg = argc
			fmt.Fprintf(stdout, "if len(%s.args) == %d {\n", vname, argc)
		}
		fmt.Fprintf(stdout, "// %s\n", lst[i].Rule.String())
		scratch = flattenRule(&lst[i].Rule, scratch[:0])
		n := emit(scratch)
		n += emitPredicates(lst[i].From[1:])
		emitCons(&lst[i].To)
		for n > 0 {
			fmt.Fprintf(stdout, "}\n")
			n--
		}
	}
	if narg != -1 {
		fmt.Fprintf(stdout, "}\n")
	}
	fmt.Fprintf(stdout, "}\n") // close switch
}

func writeRules(lst []rule) {
	orderRules(lst)
	for i := range lst {
		inputRule(&lst[i], "v")
	}

	// the generated file is only built when we are *not*
	// generating code so that we could generate broken code
	// and then re-run the code generator without manually
	// editing the broken code
	fmt.Fprintf(stdout, "//go:build !genrewrite\n")
	fmt.Fprintf(stdout, "// code generated by genrewrite.go; DO NOT EDIT\n\n")
	fmt.Fprintf(stdout, "package vm\n\n")
	fmt.Fprintf(stdout, "import \"github.com/SnellerInc/sneller/date\"\n\n")
	fmt.Fprintf(stdout, "func rewrite1(p *prog, v *value) (*value, bool) {\n")
	casematch(lst, "v")
	fmt.Fprintf(stdout, "	return v, false\n")
	fmt.Fprintf(stdout, "}\n") // close function
}

func GenrewriteMain(dst io.Writer, opnames []string, infiles []string) {
	opname = func(op ssaop) string {
		k := int(op)
		if k < len(opnames) {
			return opnames[k]
		}

		return fmt.Sprintf("%d", op)
	}

	stdout = dst
	var all []rule
	for i := range infiles {
		f, err := os.Open(infiles[i])
		if err != nil {
			fatalf("%s", err)
		}
		lst, err := rules.Parse(f)
		if err != nil {
			fatalf("%s", err)
		}
		lst = expandRules(lst)
		for i := range lst {
			op, args := splitFirst(&lst[i])

			all = append(all, rule{
				Rule: lst[i],
				op:   op,
				args: args,
			})
		}
		f.Close()
	}
	writeRules(all)
}
