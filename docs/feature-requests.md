# Feature Requests

用户 / 外部 / dev 观察里提出的功能需求。和 [bugs.md](./bugs.md) / [roadmap.md](./roadmap.md) 并列：

- `bugs.md` —— 出错了要修的
- `roadmap.md` —— 已经决定做的（MVP / Backlog 待办 / 【待调研】）
- `feature-requests.md`（本文） —— 还没决定怎么办的，从这里分流到 roadmap / bugs / ADR / rejected

每条记：

- **日期** — 提出日期（绝对日期，不要"昨天""上周"）
- **来源** — `user` / `ops` / `dev` / `外部` 等
- **诉求** — 尽量保留原话，别先翻译成工程语言
- **状态** — `pending`（待分流） / `researching`（已进 roadmap 【待调研】） / `planned`（已进 roadmap 待办或 MVP） / `in-progress` / `shipped`（附 commit id / ADR / roadmap 节点） / `rejected`（附拒绝理由）
- **去向** — 分流后指回 roadmap / bugs / ADR / commit；未分流就空着

新 request 先进 `## Open` 段；分流完成（进了 roadmap 或被拒绝）后挪到 `## Resolved`（最新在上，按日期倒序）。如果 `researching` 的调研成果落地，状态改 `shipped` 并补 commit，但条目保留在 Resolved 里作 provenance。

---

## Open

### 2026-07-05

> 以下 8 条来自同一次盘点：user 提出"我要实现一个完整的交易所，你看看现在的代码，从架构和业务上，给我列一个清单，还缺哪些东西"，dev 对照代码与 [industry-gap-analysis-2026-05-31.md](./research/industry-gap-analysis-2026-05-31.md) 梳理出交易内核之外的空白业务域。详细论证统一见 [research/full-exchange-gap-checklist-2026-07-05.md](./research/full-exchange-gap-checklist-2026-07-05.md)（下称"缺口清单"）。已有 ADR / roadmap 覆盖的内核缺口不在此重复登记。

- **[user/dev]** 链上充提 / 钱包系统 — 充值地址生成与归集、per-chain 确认数、冷热钱包分离、提现审批流（限额/延时/多签）、链上 reorg/双花处理、链上-账务对账。现状 `asset` 只有 funding wallet + 内部转账 saga，对外无 Deposit/Withdraw。接真实资金的先决条件。状态：`pending`。去向：缺口清单 §4.1。
- **[user/dev]** 法币通道（fiat on-ramp/off-ramp） — 支付渠道对接 + 法币账务。先做产品决策再谈技术。状态：`pending`。去向：缺口清单 §4.2。
- **[user/dev]** 用户账户体系 — 注册/登录、密码与 2FA、设备与会话管理、防钓鱼码、API-key 自助管理（创建/吊销/scope/IP 白名单）、母子账户（sub-account）及划转。现状"用户"只是 `user_id` 字符串 + 静态 key 文件。子账户分组是机构级 STP/SMP 的数据前提。状态：`pending`。去向：缺口清单 §4.3。
- **[user/dev]** KYC / AML / 制裁筛查 — KYC 分级与额度联动、AML 交易监测、Travel Rule、提现风控。外采/对接为主；在钱包与用户服务设计时预留 hook，实施可最后。状态：`pending`。去向：缺口清单 §4.4。
- **[user/dev]** 杠杆现货 / 借贷（margin trading + lending） — 借币下单、利息计提、维持保证金率、爆仓、借贷池/利率模型；与 ADR-0074 的 unified margin 后续路径有架构耦合（0074 的 isolated / USDT cross 已落地），需先明确关系避免两套保证金引擎。状态：`pending`。去向：缺口清单 §4.5。
- **[user/dev]** 运营与市场生命周期 — 上币流程（预热、集合竞价/开盘价保护、首日涨跌幅限制）、下架清退、公告与维护窗口、现货侧 cancel-only/reduce-only 状态编排、风险参数 staged rollout。状态：`pending`。去向：缺口清单 §4.6（与 ADR-0075 联动）。
- **[user/dev]** 财务与对账报表 — 现货手续费平台账户账本（对齐 ADR-0079 模式）、proof of reserves（储备金证明）、税务/监管导出、日终结算报表、Counter+asset+钱包三方资金守恒对账。状态：`pending`。去向：缺口清单 §4.7。
- **[user/dev]** 客服支撑工具 — 按 order_id 的全链路事件轨迹调查、资金流水导出、带审计的冲正/调账流程（走 `Transfer` + 审计，不破坏 non-goals 对 admin 直改余额的禁令）。与可观测性 correlation 查询共用地基。状态：`pending`。去向：缺口清单 §4.8。

### 2026-04-19

- **[user]** Counter / Match 核心链路改异步日志 — "counter 和 match 核心链路，日志都用异步日志，先看看 zap 支持不支持"。现状：`pkg/logx` 用 `zapcore.Lock(os.Stdout)`，每条日志在 caller goroutine 走 stdout syscall + 互斥锁，counter sequencer / match SymbolWorker 热路径被 I/O 拖。**zap 原生是否支持**：不支持"真·异步"（队列 + 后台 goroutine 写）。zap 只提供 `zapcore.BufferedWriteSyncer`（v1.20+）——属于 buffered+定期 flush：仍在 caller goroutine 写 `bufio.Writer`（加锁），后台 goroutine 仅按 `FlushInterval`（默认 30s）定期 `Sync`；默认 256KB 缓冲，满则 caller 内 flush。等价于"合并 syscall"，但 caller 仍可能被 I/O stall。**真·异步**要自己实现 `WriteSyncer`：channel / ring buffer 入队，单消费 goroutine 写 underlying sink；权衡：(a) 队列满怎么办（drop 最旧 / drop 最新 / 阻塞 caller）；(b) 进程 crash 时 tail 丢失（SIGKILL 前未 flush 的条目丢；`logger.Fatal` 路径要强制 flush 否则错误日志丢失）；(c) caller 侧字段编码是否仍同步（zap 的 `CheckedEntry.Write` 会先 `enc.EncodeEntry` 再写 sink，异步化的切点通常在 sink 之后）。状态：`pending`。去向：待分流。

## Resolved

### 2026-04-19

- **[user]** 缺管理台 / ops admin 平面 — "现在好像还少了管理台，就是一些后台的功能，比如币对上下架，按用户，按 symbol 批量撤单之类的功能？"。后续 user 反馈："分拆开，这种是对内的，和 2c 的可不能放在一起"。分流：作为 MVP-17 落地最小集 A+B+E（symbol CRUD + 批量撤单 + admin auth + JSONL 审计），C/D 推迟；**走独立 `admin-gateway` 服务**（第一版 BFF-integrated commit c8b3eb1 已废，user 明确要求进程隔离）。状态：`shipped`。去向：[ADR-0052](./adr/0052-admin-console.md) + [roadmap.md MVP-17](./roadmap.md#mvp-17-admin-console)。
- **[user]** Counter 在线 re-shard / 用户分片热迁移 — "用户和 shard 该怎么映射呢，比如一开始我规模小只有一个 shard，后面变多了，10 个 shard，原来的用户咋迁移分片呢？是在 bff 里热迁移还是怎么搞呢；redis cluster 的那个 slot 迁移机制不知道是不是能参考"。状态：`researching`。去向：[roadmap.md §填坑/Backlog §待办 【待调研】](./roadmap.md#填坑--backlog)。
- **[user]** match→user 推送消息合并策略 — 担心一笔 taker 吃 N 个 maker 时 WS 消息爆炸（200 条量级）。状态：`researching`。去向：[private-push-merge-research.md](./private-push-merge-research.md) + [roadmap.md 【待调研】](./roadmap.md#填坑--backlog)。
- **[user]** match 在 order book 找不到订单时应回消息给 counter — "match 撮合侧收到 cancel 但 book 里没这个 order 时，应该回消息给 counter 告知'不存在'"，让 counter 能 `unfreezeResidual` + 转 CANCELED。后续 user 进一步提出应成为 match 的不变量："正常和异常情况下都去 emit 一个带 seq_id 的事件"。状态：`shipped`。去向：[bugs.md §Fixed 2026-04-19](./bugs.md#fixed)。
- **[user]** Web 入口验证下单流程（替代 TUI） — "算了 tui 体验太差，你还是给我个网页入口吧"。状态：`shipped`。去向：[tools/web/](../tools/web/) + commit d9f5704。
- **[user]** 显示 PENDING_CANCEL 订单（不隐藏） — "pending_cancel 的订单，你在端上不要隐藏啊"，用户看到余额冻着却找不到对应单子。状态：`shipped`。去向：commit e601885（web 显示 pending_cancel 行 + 灰色视觉降权 + "canceling…" 占位）。
