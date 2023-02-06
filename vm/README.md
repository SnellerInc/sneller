# Sneller VM Internals

The purpose of this document is to provide a high-level introduction
to the structure and implementation of the `vm` package
so that a new user / developer can navigate the source code successfully.

## Physical Operators

Each of the "physical operators" (`Filter`, `HashAggregate`, etc.)
is defined in its own file. Each operator implements `vm.QuerySink`,
although in practice rows are typically passed between rows via
the `vm.rowConsumer` interface so that data does not have to be
fully (re-)serialized between each physical operator.

## Expressions

Operators generally accept raw AST that describes the
expression(s) to be evaluated within the operator.
For example, `Filter` accepts the expression to evaluate
and compare against `TRUE` for dropping rows to be passed
to subsequent operators. The snippets of SQL AST that
are passed to physical operators are generally verbatim
copies of the AST produced by parsing the original SQL query,
although the query planner will have taken care of lifting out
things like sub-queries.

Expressions are compiled into an SSA-based intermediate
representation that more closely represents the execution
model of our virtual machine. Most of the expression-to-SSA
compilation happens in `exprcompile.go`.

## SSA IR

The expression-to-SSA compilation uses a `prog` to collect
the results of expressions into an SSA lattice that can be
more easily optimized than the raw AST itself.
The SSA representation converts the weak-/implicitly-typed
AST into a strongly-typed representation that uses explicitly
boxing and un-boxing operations for serialized values.
The SSA representation also converts the explicit control-flow
for expressions like `CASE` into a branch-free series of
predicated operations.

After the SSA representation of an expression has been
assembled, it is converted to executable bytecode.
SSA-to-bytecode conversion and most SSA optimizations
live in `ssa.go`.

## Bytecode VM

Each bytecode VM operation is an assembly function that
conforms to a particular ABI for passing arguments, return values,
a local "stack" of saved values, a continuation address, and so forth.
The VM is always entered through a trampoline that sets up the initial
state. (See the `BC_ENTER()` macro in `bc_amd64.h`.)

Typically, the physical operators communicate with the VM
by inspecting the final stack and register state of the
program after it has executed against a vector of rows.

Currently, each bytecode operation is encoded as a two-byte instruction number
plus zero or more bytes of immediate data. The "virtual program counter"
(currently `%rax` on AMD64) points just past the current instruction at the
beginning of the bytecode routine, and the bytecode routine is expected to
advance the virtual program counter further if it receives any immediate data.
At the end of each bytecode routine, the code computes the address for the next
bytecode operation through an auto-generated look-up-table and performs a tail-call
to that address.

On AMD64, each bytecode operation operates on up to 16 rows simultaneously.
Physical operators will generally use an assembly trampoline that invokes the
same bytecode program repeatedly for batches of up to 16 rows.

The table of VM opcodes lives in `bytecode.go`,
and most of the VM implementation lives in `evalbc_amd64.s`.

## VMM (Virtual Machine Memory)

All of the serialized values addressable by the bytecode VM
live in a memory region that is reserved at program start-up.
Pages within this memory are explicitly allocated and de-allocated
with `vm.Malloc` and `vm.Free`, respectively.
There are two benefits to constraining all VM references to
living in this region:

 1. We can use 32-bit "absolute" pointers to refer to
 the address of any value, and simply compute its real
 address by adding the base address of the VMM area.
 In practice this means we can always load data from the VMM
 using `vpgather*` instructions relative to the VMM base.
 2. Out-of-bounds `vpgather*` or `vpscatter*` instructions can
 only address values within the VMM, which makes it more difficult
 to re-purpose a memory safety violation into an RCE (sensitive structures
 on the Go heap aren't easily addressable from within the VM).
 Additionally, in debug builds we are able to `mprotect` individual VM
 pages so that out-of-bounds accesses are guaranteed to trigger a segfault.
