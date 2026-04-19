# Postmortems / 事故复盘

每次严重 bug 或 outage 的叙事记录。和 [bugs.md](../bugs.md) 互补：

- [bugs.md](../bugs.md) —— "什么东西坏了 / 怎么修的"（条目式，索引用）
- `postmortems/` —— "为啥会坏到这步 / 为啥没早发现 / 系统性防御缺在哪"（叙事式 + 时间线）

## 什么时候写

够格写 postmortem：

- 影响用户可见行为 / 导致资金或状态错误 / 需要人工介入恢复
- 暴露了检测盲点（监控没告警、测试没覆盖、code review 没抓住）
- 根因跨多个服务 / 涉及竞态 / 源于上下游协议误解
- 同类问题**第二次**出现（第一次可能是偶然，第二次是系统性缺陷）

**不**够格：单点 bug，改一行就修的（只进 bugs.md）。`UNIX_TIMESTAMP DECIMAL` 那种属于单点；`PENDING_CANCEL 幽灵单` 或 `self-trade 吞一半 settlement` 那种跨多个服务 + 暴露协议缺口的属于 postmortem 对象。

## 文件命名

`YYYY-MM-DD-<short-slug>.md`

例：`2026-04-19-pending-cancel-ghost-orders.md`

## 模板

```markdown
# <标题：一句话说清现象>

- **日期**：YYYY-MM-DD
- **影响**：具体受影响范围（用户数 / 资金量 / 数据字段）
- **检测方式**：用户反馈 / 监控告警 / 自查 / smoke 发现
- **关联 bugs.md**：commit id 或锚点
- **相关 ADR**：若有

## TL;DR

3 句话：什么坏了、根因是什么、怎么修的。

## 时间线

| 时间（UTC / 本地） | 事件 |
|---|---|
| HH:MM | 现象首次出现 / 报告 |
| HH:MM | 开始排查，怀疑 X |
| HH:MM | 定位到 Y |
| HH:MM | 修复提交（commit）|
| HH:MM | 验证通过 |

## 现象

客户 / 监控 / 日志看到的外部表现。带具体 request / log 片段更好。

## 根因

真正的触发路径。区分：

- **直接原因（proximate cause）**：代码 / 协议 / 配置的具体缺陷
- **系统性原因（contributing factors）**：为什么这个缺陷能溜进来 + 没被早发现

不要把"工程师不小心"当根因。问 5 次 "why"，直到出现机制层面的回答。

## 检测盲点

为什么没更早发现？监控 / 测试 / code review / 协议设计 / 压测 / smoke 哪里有缝。

## 修复

- **短期 hotfix**：commit id + 一句描述
- **长期系统性修复**：ADR / roadmap 条目 / 协议改进

## Action items

- [ ] 加 metric / alert（具体名字）
- [ ] 加测试覆盖（具体场景）
- [ ] 改协议 / 架构（指向 ADR）
- [ ] 更新文档（bugs.md / non-goals.md / glossary.md）

每项要有 owner + 预期完成时间，否则就是废话。

## 经验

可复用的教训。避免"下次要更小心"——要具体到"哪类模式以后如何处理"。放到 feedback memory / CLAUDE.md 里是什么内容？
```

## 历史

_(none yet — 等第一次真正够格的事件)_
