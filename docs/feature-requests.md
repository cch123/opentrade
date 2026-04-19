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

_(none)_

## Resolved

### 2026-04-19

- **[user]** Counter 在线 re-shard / 用户分片热迁移 — "用户和 shard 该怎么映射呢，比如一开始我规模小只有一个 shard，后面变多了，10 个 shard，原来的用户咋迁移分片呢？是在 bff 里热迁移还是怎么搞呢；redis cluster 的那个 slot 迁移机制不知道是不是能参考"。状态：`researching`。去向：[roadmap.md §填坑/Backlog §待办 【待调研】](./roadmap.md#填坑--backlog)。
- **[user]** match→user 推送消息合并策略 — 担心一笔 taker 吃 N 个 maker 时 WS 消息爆炸（200 条量级）。状态：`researching`。去向：[private-push-merge-research.md](./private-push-merge-research.md) + [roadmap.md 【待调研】](./roadmap.md#填坑--backlog)。
- **[user]** match 在 order book 找不到订单时应回消息给 counter — "match 撮合侧收到 cancel 但 book 里没这个 order 时，应该回消息给 counter 告知'不存在'"，让 counter 能 `unfreezeResidual` + 转 CANCELED。状态：`planned`。去向：[bugs.md §Backlog](./bugs.md#backlog)（归在 bugs 因为它本质是补缺失的协议回包，不是新功能）。
- **[user]** Web 入口验证下单流程（替代 TUI） — "算了 tui 体验太差，你还是给我个网页入口吧"。状态：`shipped`。去向：[tools/web/](../tools/web/) + commit d9f5704。
- **[user]** 显示 PENDING_CANCEL 订单（不隐藏） — "pending_cancel 的订单，你在端上不要隐藏啊"，用户看到余额冻着却找不到对应单子。状态：`shipped`。去向：commit e601885（web 显示 pending_cancel 行 + 灰色视觉降权 + "canceling…" 占位）。
