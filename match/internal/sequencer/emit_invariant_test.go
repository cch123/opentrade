package sequencer

// ARCH-001: match 每个 input 必须产出带 match_seq 的 output。
//
// 规则：`*SymbolWorker` 上任何 `handle*` 方法（dispatcher `handle` 除外）的每个显式
// `return` 语句，都必须在同一执行路径上有一个前置的 `w.emit(...)` 直接调用。
// 违反 = 下游（counter / quote / trade-dump）可能拿不到 match_seq 字段，
// 无法做 dedup / 无法关闭 PENDING_CANCEL 之类的协议回合。
//
// 动机 / 历史：见 docs/bugs.md 2026-04-19 cancel-miss bug + docs/arch-guards.md ARCH-001。
//
// 分析范围：
//  - 仅当前 package (sequencer)，因为 emit 的 receiver 是 *SymbolWorker
//  - 词法级 must-analysis（不是严格 CFG）：track "在走到当前语句之前，是否在同一块
//    或祖先块的前置语句里看到过一次直接 emit"。for / range 体内的 emit 不对外传播
//    （0 次迭代假设），if / switch / select 各分支独立分析并继承父块状态
//  - panic / goto / break / continue 不特别处理：目的是抓"显式 return 之前没 emit"，
//    其它退出方式（panic）不会让下游卡住
//  - 函数尾部隐式 return 不检查（绝大多数 handler 最后一个 case 都显式 emit 过）
//
// 绕过：`// arch:ARCH-001-ok <reason>` 到 return 同一行。绕过必须给出理由。

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestMatchEmitInvariant scans the sequencer package source and asserts that
// every *SymbolWorker handler method emits before returning. New handlers added
// to the worker are automatically covered (discovery is by name prefix).
func TestMatchEmitInvariant(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		// Skip test files — we only lint production code.
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	pkg, ok := pkgs["sequencer"]
	if !ok {
		t.Fatal("sequencer package not found in current dir")
	}

	var violations []string
	for _, f := range pkg.Files {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !isSymbolWorkerHandler(fn) {
				continue
			}
			recvName := fn.Recv.List[0].Names[0].Name
			for _, v := range findEmitViolations(fn, recvName, fset, f) {
				violations = append(violations, v)
			}
		}
	}
	for _, v := range violations {
		t.Errorf("match emit invariant violated: %s", v)
	}
}

// TestMatchEmitInvariantDetectsRegression sanity-checks the linter itself:
// a synthetic handler with a bare return must be flagged. If this ever starts
// reporting zero violations, the linter is broken and real regressions would
// slip through.
func TestMatchEmitInvariantDetectsRegression(t *testing.T) {
	src := `package sequencer

type bogusWorker struct{}

func (w *SymbolWorker) handleBogus(evt *Event) {
	if evt == nil {
		return // should be flagged: no emit before return
	}
	w.emit(&Output{})
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "synthetic.go", src, 0)
	if err != nil {
		t.Fatalf("parse synthetic: %v", err)
	}
	fn := f.Decls[1].(*ast.FuncDecl)
	viols := findEmitViolations(fn, "w", fset, f)
	if len(viols) == 0 {
		t.Fatal("linter failed to detect bare-return regression")
	}
}

// TestMatchEmitInvariantAllowsAnnotation verifies the // arch:ARCH-001-ok
// escape hatch. Used when a handler legitimately returns without emit
// (e.g. the dispatcher itself, or a path that panics).
func TestMatchEmitInvariantAllowsAnnotation(t *testing.T) {
	src := `package sequencer

func (w *SymbolWorker) handleAnnotated(evt *Event) {
	if evt == nil {
		return // arch:ARCH-001-ok dispatcher-like routing guard
	}
	w.emit(&Output{})
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "synthetic.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse synthetic: %v", err)
	}
	fn := f.Decls[0].(*ast.FuncDecl)
	viols := findEmitViolations(fn, "w", fset, f)
	if len(viols) != 0 {
		t.Errorf("annotated return should be allowed: %v", viols)
	}
}

// -----------------------------------------------------------------------------
// Implementation
// -----------------------------------------------------------------------------

func isSymbolWorkerHandler(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || len(fn.Recv.List) == 0 || len(fn.Recv.List[0].Names) == 0 {
		return false
	}
	star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	ident, ok := star.X.(*ast.Ident)
	if !ok || ident.Name != "SymbolWorker" {
		return false
	}
	name := fn.Name.Name
	// handle = top-level dispatcher (routes to handleXxx). emit = the thing
	// we're checking for. Both return without emitting themselves.
	if name == "handle" || name == "emit" {
		return false
	}
	return strings.HasPrefix(name, "handle")
}

func findEmitViolations(fn *ast.FuncDecl, recvName string, fset *token.FileSet, file *ast.File) []string {
	if fn.Body == nil {
		return nil
	}
	var violations []string
	annotations := collectEmitOkLines(file, fset)

	var walk func(block *ast.BlockStmt, alreadyEmitted bool)
	walk = func(block *ast.BlockStmt, alreadyEmitted bool) {
		emitted := alreadyEmitted
		for _, stmt := range block.List {
			if isDirectEmit(stmt, recvName) {
				emitted = true
			}
			if ret, ok := stmt.(*ast.ReturnStmt); ok && !emitted {
				pos := fset.Position(ret.Pos())
				if !annotations[pos.Line] {
					violations = append(violations, fmt.Sprintf(
						"%s: %s: return with no preceding w.emit() — "+
							"match requires every input to produce an output with match_seq "+
							"(see docs/arch-guards.md ARCH-001 + docs/bugs.md 2026-04-19 cancel-miss). "+
							"If intentional, annotate the return with `// arch:ARCH-001-ok <reason>`.",
						pos, fn.Name.Name))
				}
			}
			recurseChildBlocks(stmt, emitted, walk)
		}
	}
	walk(fn.Body, false)
	return violations
}

// isDirectEmit matches `w.emit(...)` where w is the receiver name. A purely
// syntactic check — if someone wraps emit in a helper, the linter won't see
// through it. For now this matches the codebase; revisit if helpers emerge.
func isDirectEmit(stmt ast.Stmt, recvName string) bool {
	expr, ok := stmt.(*ast.ExprStmt)
	if !ok {
		return false
	}
	call, ok := expr.X.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return ident.Name == recvName && sel.Sel.Name == "emit"
}

// recurseChildBlocks walks into the nested block(s) of a control-flow statement
// with inherited "emitted so far" state. For for/range, body emits do NOT
// propagate out (0 iterations is possible); the caller already excludes them.
func recurseChildBlocks(stmt ast.Stmt, emittedBefore bool, walk func(*ast.BlockStmt, bool)) {
	switch s := stmt.(type) {
	case *ast.IfStmt:
		walk(s.Body, emittedBefore)
		switch e := s.Else.(type) {
		case *ast.BlockStmt:
			walk(e, emittedBefore)
		case *ast.IfStmt:
			recurseChildBlocks(e, emittedBefore, walk)
		}
	case *ast.ForStmt:
		walk(s.Body, emittedBefore)
	case *ast.RangeStmt:
		walk(s.Body, emittedBefore)
	case *ast.SwitchStmt:
		for _, cc := range s.Body.List {
			if cl, ok := cc.(*ast.CaseClause); ok {
				walk(&ast.BlockStmt{List: cl.Body}, emittedBefore)
			}
		}
	case *ast.TypeSwitchStmt:
		for _, cc := range s.Body.List {
			if cl, ok := cc.(*ast.CaseClause); ok {
				walk(&ast.BlockStmt{List: cl.Body}, emittedBefore)
			}
		}
	case *ast.SelectStmt:
		for _, cc := range s.Body.List {
			if cl, ok := cc.(*ast.CommClause); ok {
				walk(&ast.BlockStmt{List: cl.Body}, emittedBefore)
			}
		}
	case *ast.BlockStmt:
		walk(s, emittedBefore)
	}
}

// collectEmitOkLines finds every source line carrying an `arch:ARCH-001-ok`
// escape-hatch comment. Returning from such a line is allowed without a
// preceding emit.
func collectEmitOkLines(file *ast.File, fset *token.FileSet) map[int]bool {
	out := map[int]bool{}
	for _, cg := range file.Comments {
		for _, c := range cg.List {
			if strings.Contains(c.Text, "arch:ARCH-001-ok") {
				out[fset.Position(c.Slash).Line] = true
			}
		}
	}
	return out
}
