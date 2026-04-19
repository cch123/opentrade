# Arch Guards / 架构约束

项目里必须守住的**架构不变量**。不是通用 bug（那些交给 staticcheck / golangci-lint），而是**本项目特有的结构 / 协议 / 领域约束** —— 违反一次不一定能立刻发现，但积累起来会把架构腐蚀掉，或者破坏某个显式依赖链上的前置条件。

**为什么要有**：这类约束 traditional static analysis 覆盖不了（编译器 + 常规 lint 都过不了，但系统坏了）。ADR 记录的是"决定做 X"；本文是 ADR 决定之后"为保证 X 能成立，每次改代码都必须守住的条件"。

**和其他文档的关系**：

- [ADR](./adr/) —— 决策本身
- [non-goals.md](./non-goals.md) —— 不做 Y
- [bugs.md](./bugs.md) —— 违规导致的实际事故
- **本文** —— 每条决策背后的运行时不变量 + 如何守住

---

## 条目格式

每条规则：

```markdown
### ARCH-NNN: 规则名

- **范畴**：A 依赖图 / B 结构 / C 流内契约 / D 命名 / E 跨切面语义 / R 运行时断言
- **规则**：一句话说清不变量
- **动机**：违反会怎样（引用 ADR / bugs / 实际事故）
- **实现**：具体在哪里（文件路径 / 触发方式 / escape hatch）
- **添加日期**：YYYY-MM-DD

*(可选)* 补充说明：边界、已知误报、未来扩展
```

## 绕过约定

每条规则都支持**明文绕过**，但必须给理由：

```go
return // arch:<rule-id>-ok <reason>
```

示例：`// arch:ARCH-001-ok dispatcher routes events, doesn't produce output itself`

**绕过必须在 PR review 时讨论过**。无理由的 `arch:*-ok` 跟没有标注一样，下一位 reviewer 该 reject 就 reject。

## 检查分层

| 类型 | 实现方式 | 触发时机 | 例子 |
|---|---|---|---|
| AST lint | `go/parser` + `go/ast` 自写 | `go test` | ARCH-001, ARCH-002 |
| 依赖图 | depguard / go-arch-lint | `go vet` / CI | 未来 |
| 类型级 | `go/analysis` + `go/types` | 专用 analyzer 二进制 | 未来 |
| 运行时断言 | 代码里的 `panic(...)` | 命中坏路径时 | 未来 |
| property-based | testing/quick + go-gopter | `go test` | 未来 |

原则：**能在 `go test` 里跑就在里面跑** —— dev 每天都会注意到 test 红；CI-only 的 lint 更容易被忽略。

---

## 当前规则

### ARCH-001: match 每个 input 必须产出带 match_seq 的 output

- **范畴**：C 流内契约
- **规则**：`*SymbolWorker` 上任何 `handle*` 方法的每个 `return` 必须有前置 `w.emit(...)` 直接调用
- **动机**：match 静默吞掉 cancel 事件会让 counter 永远停在 PENDING_CANCEL，frozen 资金卡死。见 [bugs.md](./bugs.md#fixed) 2026-04-19 的 cancel-miss bug 和 [ADR-0048](./adr/0048-snapshot-offset-atomicity.md) 的 output flush barrier 语义
- **实现**：[match/internal/sequencer/emit_invariant_test.go](../match/internal/sequencer/emit_invariant_test.go) 里 `TestMatchEmitInvariant`。AST 词法级 must-analysis。新加 `handleXxx` 自动覆盖（按名字前缀发现）
- **绕过**：`// arch:ARCH-001-ok <reason>`（已验证测试：`TestMatchEmitInvariantAllowsAnnotation`）
- **添加日期**：2026-04-19

限制：词法级不是严格 CFG；for/range 体内的 emit 不向外传播（0 次迭代假设）。handlePlaced 的 TakerFilled 非 quote-driven 路径靠"`len(Trades) > 0` 是 engine 的保证"逻辑上成立但 linter 看不出来 —— 暂时靠隐式 fall-through（无显式 return）绕过，未来可以在 engine 里加一条 panic 断言让它形式化可证。

### ARCH-002: match engine 内不得引入非确定性

- **范畴**：C 流内契约
- **规则**：`match/internal/engine` 的生产代码（非 `_test.go`）不准 import/调用 `time.Now` / `time.Since` / `time.Until` / `time.After` / `math/rand` / `crypto/rand` / `os.Getenv` / `os.Environ` / `runtime.GOOS` 这类把"环境状态"引入执行路径的函数
- **动机**：ADR-0048 的 exactly-once 语义链路依赖**确定性重放** —— 同一序列 input 重新喂进 match，必须产出字节级（match_seq / trade_id）相同的 output。一旦 engine 读了墙钟 / 随机 / env，重启时重放出来的 seq 和历史对不上，counter 的 per-(user, symbol) `LastMatchSeq` guard 会把真事件错 dedup 成重放，账户直接错账。参考 2026-04-19 session 讨论里 "为什么下游 dedup 能 work"
- **实现**：[match/internal/engine/determinism_test.go](../match/internal/engine/determinism_test.go) 里 `TestEngineDeterminism`。AST 扫 call expression + import path
- **绕过**：`// arch:ARCH-002-ok <reason>`。允许的情况极少：订单的 `CreatedAt` 时间戳来自 input event，不是 engine 自己调 Now
- **添加日期**：2026-04-19

限制：package 粒度 —— 只管 engine 自己。下游调用者（sequencer / journal）引入 time.Now 不影响 engine 确定性，不在本规则范围。

---

## 计划但未实现的规则

按 ROI 排列，记作 backlog：

### ARCH-003（planned）：服务间不得跨 `internal/` 依赖

- **范畴**：A 依赖图
- **规则**：`counter/internal/*` 不准被 `bff` / `match` / `quote` 等其它服务 import；`api/` proto 是服务间的唯一契约
- **动机**：防止服务悄悄耦合，破坏"Kafka 是唯一跨服务通道"的架构前提
- **候选实现**：`tools/archlint/` 独立二进制，或 `pkg/archlint/` 共享库 + 每个服务 `arch_test.go` 调用

### ARCH-004（planned）：snapshot Capture 前必须先 flush producer

- **范畴**：C 流内契约 + 跨 pkg
- **规则**：任何 `snapshot.Save` / `snapshot.Capture` 之前同一函数里必须有 `producer.FlushAndWait` 调用
- **动机**：ADR-0048 output flush barrier 语义 —— 不 flush 就落 snapshot 会留下"state 前进了、output 还在内存 buffer 里没 commit 到 Kafka"的窗口，重启会丢事件
- **候选实现**：`go/analysis` analyzer（需要 type info 看清楚方法是不是同一个 producer 对象）

### ARCH-005（planned）：金额必须用 `dec.Decimal`，禁用 `float64` / `float32`

- **范畴**：E 跨切面语义
- **规则**：涉及 balance / price / qty / fee 的字段必须 `dec.Decimal`
- **动机**：float 不是精确的，累加 1000 次 0.1 能跑出 100.00000000000023；交易系统直接爆
- **候选实现**：`ruleguard` / `go/analysis` + 按字段名前缀白名单

### ARCH-006（planned）：engine 里 `TakerFilled ⇒ len(Trades) > 0` 的 panic 断言

- **范畴**：R 运行时断言
- **规则**：engine.Match 返回 `TakerFilled` 时必须 `len(res.Trades) > 0`，否则 panic
- **动机**：既文档化这个语义不变量，又让 ARCH-001 的 handlePlaced TakerFilled 非 quote-driven 路径形式化可证（因为 Trade emit 在循环里，引擎保证循环至少跑一次）
- **候选实现**：engine.Match 返回前一行 `if res.Status == TakerFilled && len(res.Trades) == 0 { panic(...) }`

### ARCH-007（planned）：counter 每个 trade-event handler 必须同时有 matchSeq guard + AdvanceMatchSeq

- **范畴**：C 流内契约
- **规则**：counter/internal/service/trade.go 里每个 handle* 方法的 sequencer callback 里必须成对出现 `matchSeqDuplicate` + `AdvanceMatchSeq`
- **动机**：少一个 guard = 重放时双重应用；少一个 advance = 水位永远追不上，后续事件全被 dedup 掉
- **候选实现**：AST lint 同 ARCH-001 模式

### ARCH-008（planned）：`X-User-Id` header 只能在 `--auth-mode=header` 下生效

- **范畴**：E 跨切面语义 / 安全
- **规则**：bff 的 middleware 里 `X-User-Id` 绝对不能跳过 auth 检查
- **动机**：生产 jwt / api-key 模式下如果意外允许 header bypass，等于全盘越权
- **候选实现**：`go/analysis` analyzer 或集成测试

---

## 新增规则流程

1. 发现某个 bug / review / 讨论点出来"这种错不该发生第二次"
2. 评估能不能静态抓：落成 arch guard；抓不住的降级到 runtime 断言 or doc-only 警告
3. 选最低代价的实现层（AST > go/analysis > 自写 SSA）
4. **同一 commit 里**：加 rule 实现 + 在本文登记 + 加 `TestXxxDetectsRegression` 的合成违规样本
5. 绕过条件（`// arch:<id>-ok`）从一开始就考虑，不要上线之后才 retrofit

## 调整规则

如果规则本身过时了：

- 先在本文把条目标 `DEPRECATED`，给出原因 + 替代方案
- 给一个过渡期，让在用的 `arch:*-ok` 绕过注释可以慢慢清
- 最后 PR 里删掉 rule 实现 + 本文条目

## 不要做什么

- ❌ 规则太多 → 全部被忽略。一页能看完，超过 20 条就考虑是不是可以合并或降级为 doc
- ❌ 没有绕过机制 → 总有合法例外；强制合规只会让 dev 绕开 linter
- ❌ 绕过没 reviewer 把关 → 退化成自动 `nolint`
- ❌ 规则实现没自测（`TestDetectsRegression`）→ 某天 linter 静默失效，真 bug 溜过
- ❌ 只在 CI 跑 → dev 本地不知道违规，PR 才发现，来回折腾
