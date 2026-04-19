package engine

// ARCH-002: match engine determinism guard.
//
// 规则：本 package 的生产代码（非 _test.go）不准 call / import 任何把"环境状态"
// 带进执行路径的东西：
//   - time.Now / time.Since / time.Until / time.After / time.Tick
//   - math/rand.* / crypto/rand.*
//   - os.Getenv / os.Environ / os.Hostname
//   - runtime.GOOS / runtime.NumCPU 这类
//
// 动机：ADR-0048 的 EOS 链路依赖 match 的确定性重放 —— 同一 input 序列必须产出
// 字节级（match_seq / trade_id）相同的 output，否则 counter 的 per-(user,symbol)
// LastMatchSeq guard 会把真事件错当重放。一旦 engine 读墙钟 / 随机 / env，账户直接错账。
//
// 见 docs/arch-guards.md ARCH-002。
//
// 绕过：`// arch:ARCH-002-ok <reason>`。合法情况极少 —— 订单的 CreatedAt / TS 是从
// input event 带入的，engine 自己不应该问"现在几点"。

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// forbiddenCalls maps "package.function" patterns that the engine must not call.
// Match against *ast.SelectorExpr: ident.Name + "." + sel.Name.
var forbiddenCalls = map[string]string{
	"time.Now":     "wall-clock breaks deterministic replay",
	"time.Since":   "wall-clock breaks deterministic replay",
	"time.Until":   "wall-clock breaks deterministic replay",
	"time.After":   "wall-clock breaks deterministic replay",
	"time.Tick":    "wall-clock breaks deterministic replay",
	"time.Unix":    "wall-clock breaks deterministic replay (unless arg is passed-in timestamp)",
	"rand.Int":     "non-determinism breaks replay",
	"rand.Intn":    "non-determinism breaks replay",
	"rand.Float64": "non-determinism breaks replay",
	"rand.Read":    "non-determinism breaks replay",
	"rand.Seed":    "non-determinism — engine must not reseed",
	"os.Getenv":    "env state breaks replay (same build behaves differently)",
	"os.Environ":   "same as Getenv",
	"os.Hostname":  "host-specific output breaks replay",
}

// forbiddenImports declares whole-package forbids.
var forbiddenImports = map[string]string{
	"math/rand":   "non-determinism (use dec / fixed-point if you need pseudo-random)",
	"crypto/rand": "non-determinism (only acceptable at auth / key-gen boundary)",
}

func TestEngineDeterminism(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	pkg, ok := pkgs["engine"]
	if !ok {
		t.Fatal("engine package not found")
	}

	var violations []string
	for _, f := range pkg.Files {
		ok := collectArchOkLines(f, fset, "ARCH-002")
		violations = append(violations, findDeterminismViolations(f, fset, ok)...)
	}
	for _, v := range violations {
		t.Errorf("ARCH-002 violated: %s", v)
	}
}

// TestEngineDeterminismDetectsRegression sanity-checks the linter.
func TestEngineDeterminismDetectsRegression(t *testing.T) {
	src := `package engine

import "time"

func Bogus() int64 {
	return time.Now().UnixNano()
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "synthetic.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	v := findDeterminismViolations(f, fset, nil)
	if len(v) == 0 {
		t.Fatal("linter failed to detect time.Now call")
	}
}

// TestEngineDeterminismAllowsAnnotation verifies escape hatch.
func TestEngineDeterminismAllowsAnnotation(t *testing.T) {
	src := `package engine

import "time"

func Allowed() int64 {
	return time.Now().UnixNano() // arch:ARCH-002-ok only used in test fixture builder
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "synthetic.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ok := collectArchOkLines(f, fset, "ARCH-002")
	v := findDeterminismViolations(f, fset, ok)
	if len(v) != 0 {
		t.Errorf("annotated call should be allowed: %v", v)
	}
}

// -----------------------------------------------------------------------------
// Implementation
// -----------------------------------------------------------------------------

func findDeterminismViolations(f *ast.File, fset *token.FileSet, okLines map[int]bool) []string {
	var out []string
	// Import check
	for _, imp := range f.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		if reason, bad := forbiddenImports[path]; bad {
			pos := fset.Position(imp.Pos())
			if !okLines[pos.Line] {
				out = append(out, fmt.Sprintf("%s: import %q forbidden: %s (annotate with // arch:ARCH-002-ok <reason> if intentional)",
					pos, path, reason))
			}
		}
	}
	// Call-site check
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		key := ident.Name + "." + sel.Sel.Name
		if reason, bad := forbiddenCalls[key]; bad {
			pos := fset.Position(call.Pos())
			if !okLines[pos.Line] {
				out = append(out, fmt.Sprintf("%s: call to %s forbidden in match engine: %s (annotate with // arch:ARCH-002-ok <reason> if intentional)",
					pos, key, reason))
			}
		}
		return true
	})
	return out
}

// collectArchOkLines returns the set of line numbers carrying the given rule's
// escape-hatch annotation (e.g. "arch:ARCH-002-ok ...").
func collectArchOkLines(file *ast.File, fset *token.FileSet, ruleID string) map[int]bool {
	out := map[int]bool{}
	needle := "arch:" + ruleID + "-ok"
	for _, cg := range file.Comments {
		for _, c := range cg.List {
			if strings.Contains(c.Text, needle) {
				out[fset.Position(c.Slash).Line] = true
			}
		}
	}
	return out
}
