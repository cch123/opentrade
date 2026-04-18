# Bug Tracking

按时间倒序记录项目里发现并修复 / 待修复的 bug。每条：

- **短描述** — 症状
- **Commit** — 修复落地的 git commit（`git show <id>` 可看细节）
- **根因** — 一句话解释
- **状态** — `fixed` / `open` / `workaround`

新 bug 先写进 `## Open` 段；修完后挪到 `## Fixed`（最新在上）、带上 commit id。仅调研还没动手的可以放在 `## Backlog`。

---

## Open

_(none)_

## Backlog

- **match 撮合侧收到 cancel 但 book 里没这个 order 时，应该回消息给 counter 告知 "不存在"** — 让 counter 能执行 `unfreezeResidual` + 转 CANCELED 终态。当前 match 静默丢弃，counter 永远停在 PENDING_CANCEL，frozen 资金卡死。这是之前 dev 多次重启导致 counter / match 状态分叉后的根本修复；不修的话只能靠人工 force-cancel 或清状态。

## Fixed

### 2026-04-19

- **取消订单时 open orders 闪烁出更多单又消失** — `pollAccount` 多路径并发（cancel 回调 / WS user event / 2.5s 定时器），更老的 poll 结果覆盖了更新的一次。
  - commit: [`7fc4089`](../../commit/7fc4089)
  - 修法：给每次 pollAccount 加单调 seq，写回前比对，落后的 drop。

- **orderbook 刷新页面就没了** — web 只订阅 WS `depth@` 增量，Quote 每 5s 才推一次 snapshot，页面首屏空窗。
  - commit: [`057b4ef`](../../commit/057b4ef)
  - 修法：boot 时 `GET /v1/depth/<symbol>` 拉 BFF market-cache 的种子（ADR-0038）；`smoke.sh` 给 BFF 加 `--market-brokers`。

- **counter 对自成交 (maker_user == taker_user) 只应用了一半 settlement** — `handleTrade` 两次调 `applyPartyViaSequencer`，同一账户同一 matchSeq，第一次 advance 后第二次被 match_seq 保护直接跳过。
  - commit: [`4ccaf23`](../../commit/4ccaf23)
  - 修法：检测到同用户两边，走 `applySelfTrade` 合并路径，在一次 sequencer.Execute 里顺序应用两边 settlement，末尾一次 AdvanceMatchSeq。附 2 个单测。

- **cancel 后刷新页面又回来了（PENDING_CANCEL 幽灵单）** — BFF 把 internal `PENDING_CANCEL` 折叠成外部 `new`，UI 区分不出"在途取消"和"正常挂单"；叠加 counter/match 状态分叉导致永远完不成 CANCELED 转移。
  - commit: [`9eb713a`](../../commit/9eb713a)
  - 修法：`/v1/order/{id}` 响应加 `internal_status` 字段暴露 8 态原始值，web dev UI 在 counter-truth 校验时过滤 `pending_cancel`。
  - 注意：已按用户反馈改为显示而非隐藏（见更新条目）。

- **accidentally committed tui binary** — `tools/tui/tui` 把 10MB Mach-O 一起塞进了 git。
  - commit: [`8f1d21d`](../../commit/8f1d21d)
  - 修法：删文件 + `.gitignore` 加 `tools/*/tui`、`tools/*/web`。

- **history `/v1/orders` 永远 500 — sql: Scan error converting []uint8 to int64** — MySQL 8 里 `UNIX_TIMESTAMP(t)*1000 + MICROSECOND(t) DIV 1000` 返回 DECIMAL，driver 给 Go 的是 []byte，int64 扫描崩。
  - commit: [`5276376`](../../commit/5276376)
  - 修法：表达式外面套 `CAST(... AS SIGNED)` 强制整型。
