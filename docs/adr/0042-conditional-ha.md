# ADR-0042: 条件单服务 HA（cold standby）

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0031, 0040, 0041

## 背景 (Context)

[ADR-0040](./0040-conditional-order-service.md) 把 conditional 作为单实
例服务发布。触发延迟是秒级，单点故障的后果：

- 某个 symbol 突然暴动时，primary crash 正好发生在触发窗口
- 所有 pending 条件单要等 operator 手动重启 + snapshot 恢复后才能触发
  —— 几十秒到几分钟不等
- 用户视角：止损没生效

Counter 和 Match 已经在 [ADR-0031](./0031-ha-cold-standby-rollout.md)
里解决了同类问题：etcd leader election + cold standby，primary crash 后
backup 在 lease TTL 内接管。conditional 的状态模型（本地 JSON snapshot
+ Kafka offset，ADR-0036 同款）和 Counter/Match 高度相似，完全可以复用
同一套骨架。

## 决策 (Decision)

### 1. 一套 etcd key、两条进程、一个角色切换

增加两个运行模式：

```
--ha-mode=disabled   # 默认，单实例行为不变（MVP-14a/b）
--ha-mode=auto       # 走 etcd leader election
```

`auto` 下两个 conditional 进程共用一个 etcd key：

```
/cex/conditional/leader
```

只有 primary 会：

- 打开 gRPC `ConditionalService` 监听 `--grpc-addr`
- 订阅 `market-data` Kafka topic 并触发条件单
- 跑 snapshot ticker 写磁盘

backup 静默等 `elec.LostCh()` / `rootCtx.Done()`。primary 失联 → 新 primary
在 lease TTL (默认 10s) 内赢取选举，从本地 snapshot 恢复状态 + 从 saved
offset 继续消费 market-data。

### 2. 进程结构复刻 ADR-0031

```
main() {
  if ha-mode == disabled: runPrimary(ctx)
  else: runElectionLoop(ctx)
}

runElectionLoop(ctx):
  loop {
    Campaign(ctx)  // blocks until we win
    primaryCtx, cancel := ctx.WithCancel(ctx)
    go watchLost(elec, cancel)
    runPrimary(primaryCtx)  // full stack
    // returns on primaryCtx cancel (either LostCh or shutdown)
  }

runPrimary(ctx):
  // everything that used to be in main() body
  // dial counter, restore snapshot, start consumer, serve gRPC
  // block until ctx.Done()
  // graceful shutdown + final snapshot
```

Counter 的 `runPrimary` 签名是 `(context.Context, Config, *zap.Logger)`；
conditional 直接照抄，减少视觉差异。

### 3. 状态在哪里汇合

四条状态，依赖顺序：

1. **Kafka offsets**：snapshot 里 per-partition；primary 读 snapshot 后从
   saved offset 续消费 → Quote 的 PublicTrade 流不中断
2. **Pending / terminal conditionals**：snapshot 里的 JSON；primary 启动
   时 `snapshot.Restore` 灌进 engine
3. **Counter 上的 reservations**（MVP-14b / ADR-0041）：归 Counter 管，
   两个 conditional 实例共享同一份 Counter reservation 表。primary 切换
   不影响 reservation：触发时 PlaceOrder(reservation_id) 照常走
4. **Snapshot 文件的物理位置**：两个实例必须能读到同一份。MVP 按
   ADR-0031 §5 的约定，用共享 mount（NFS / EFS）。`--snapshot-dir` 指向
   共享目录；两个进程都能 read/write

### 4. 边界：无 fencing token

Conditional 不往 counter-journal 写事件（ADR-0040 §3），没有 Kafka
transactional.id，因此也没有 ADR-0017 风格的 fencing token 需求。唯一
的外部写通道是调用 Counter.PlaceOrder / Reserve / ReleaseReservation
—— 这些 RPC 本身用 `reservation_id` / `client_order_id` 做幂等键，
两个 primary 同时发的情况下 Counter 的 dedup 表会把第二次吸收掉，不会
双重下单。

也就是说，split-brain 窗口（两个 primary 同时认为自己是 leader）在
conditional 里退化为"浪费几次 RPC，不产生错误"，远弱于 Counter/Match
的 trade-event 双写风险，不需要额外 fencing。

## 备选方案 (Alternatives Considered)

### A. Active-active 多实例 + partition 分片
像 Push（ADR-0033）一样按 user / symbol 分片，每个 conditional 实例只处
理一部分。

- 优点：无 failover 延迟，规模扩展线性
- 缺点：
  - 需要按 user_id（或 cond_id）分 partition，市场数据 fan-out 要做 fil
    ter（和 ADR-0033 一样）
  - 条件单触发要跨分片协调（同一用户的两条 conditional 触发可能落到不
    同实例）
  - 实现量远大于 cold standby
- 结论：条件单量级不会到单机撑不住的程度（条件单总量 < 订单总量），先
  不做

### B. 不做 HA，靠"重启快"
- 优点：零代码
- 缺点：重启期间 pending 条件单无人照看，用户止损单可能"睡过去"最糟糕
  的几秒
- 结论：MVP-14a/b 就是这样，14c 的目的就是补

### C. 直接写 counter-journal 事件 + 事务 fencing
- 把 conditional 的状态变更也写进 Kafka
- 优点：和 Counter 一样严格的分布式一致性
- 缺点：大改 proto + trade-dump，产生大量对业务无意义的 journal 事件
- 结论：conditional 的操作 idempotency（reservation_id + client_order_id）已经
  足够，不需要这层重叠保护

### D. 以 Quote 为参照，单实例不做 HA
Quote 也只是单实例（ADR-0036 没做 HA）。

- Quote 和 conditional 差别：Quote 挂了下游 Push/BFF-marketcache 只是
  暂停更新，不产生"应该触发但没触发"的后果；conditional 挂了 = 用户止
  损失灵，金钱直接损失
- 结论：conditional 优先级高于 Quote，HA 必要

## 理由 (Rationale)

- **复用即 win**：`pkg/election` 已经在 Counter/Match 生产级验证过；
  conditional 的进程结构本来就和 Counter 对齐；直接套用
- **失败半径可控**：cold standby 下新 primary 接管需要 lease TTL (10s)
  + snapshot 读 + 启 consumer（~1-2s），10-15s 内恢复触发。比手动重
  启快一个数量级
- **零 Counter 改动**：reservation 的存在让 primary 切换几乎"无状
  态"——触发时 Counter 只关心 reservation_id 存不存在，不关心是哪个
  conditional 进程发来的

## 影响 (Consequences)

### 正面

- primary crash 窗口内，新 primary 几秒内接管，pending 条件单照常触发
- 运维语言统一：`--ha-mode=auto --etcd=...` 和 Counter/Match 一致，部署
  脚本可以共用 Helm chart / systemd unit 模板
- HA 开关独立：`--ha-mode=disabled` 保留 MVP-14a/b 的单机行为，CI /
  本地开发不需要 etcd

### 负面 / 代价

- 必须 mount 共享存储到 `--snapshot-dir`，否则 backup 拿不到 primary 的
  最新 snapshot（和 ADR-0031 §5 同样的约束）
- 两个 conditional 进程共享同一个 Kafka consumer group id →
  `AdjustFetchOffsetsFn` 的行为和单实例一致，但如果某天 etcd 故障导致
  两个都认为自己是 primary：两个同时 poll 同一 group 会让 Kafka 做
  partition rebalance，消息分摊 → 状态局部停止推进。市场数据的丢失不
  是错误，只是"暂时漏触发"，backup 接管后通过 replay 补齐
- snapshot 磁盘 I/O 每 30s 一次；两个实例都跑 ticker 会在共享盘上产生
  互相覆盖的 race —— 实际上只有 primary 跑 ticker（`runPrimary` 独占），
  backup 不写

### 中性

- `--ha-mode=auto` 引入了 etcd 依赖。conditional 的可用性下限现在是
  `min(kafka, etcd, counter)`。和 Counter/Match 对齐，monitoring 已有

## 实施约束 (Implementation Notes)

### 代码改动

- `conditional/cmd/conditional/main.go`：
  - 把原 `main()` 体抽成 `runPrimary(ctx, cfg, logger)`
  - 新 `runElectionLoop(ctx, cfg, logger)`：`pkg/election` 封装
  - `main()` 按 `cfg.HAMode` 分派
  - Config 增加 HAMode / EtcdEndpoints / ElectionPath / LeaseTTL /
    CampaignBackoff；`validate()` 检查 `ha-mode=auto` 必须带 `--etcd`

### Flag 清单（新增）

```
--ha-mode          disabled | auto    (default disabled)
--etcd             host1:2379,host2:2379
--election-path    /cex/conditional/leader
--lease-ttl        10     (seconds)
--campaign-backoff 2s
```

### 失败场景

| 场景 | 结果 |
|---|---|
| primary crash (kill -9) | lease 过期后（≤ LeaseTTL）backup 赢得选举，从 snapshot 恢复 |
| etcd 暂时不可达（primary 在位时） | primary 继续跑（lease 仍在）；backup 无法 campaign，保持 standby；etcd 恢复后行为回归正常 |
| 两个 primary（split-brain） | Kafka consumer group rebalance 切走一部分 partition；Counter 侧用 reservation_id / client_order_id dedup 吸收重复 PlaceOrder。无业务错误，但"暂时漏触发" |
| snapshot 共享盘挂掉 | primary 继续跑（保持内存状态）；backup 启动时 Restore 失败 → `runPrimary` 早退出 → 重回 campaign 循环。backup 会一直失败直到盘恢复 |
| etcd 永久不可达（`--ha-mode=auto` 启动时） | `election.New` 失败 → Fatal。用 `--ha-mode=disabled` 可以强制启动跳过选举 |

## 未来工作 (Future Work)

- **多 region / zone 感知**：两个实例一个 zone 时同时挂概率相关；未来
  让 election prefer cross-zone peer
- **snapshot 搬到 S3**：和 ADR-0036 § 未来工作合并，shared mount → S3
  对象存储，跨主机更透明
- **backup 周期 warm-up**：让 backup 也订 market-data（只读 offsets，不
  触发），primary 切换时接管更快。目前 cold standby 启动时才开 consumer
- **partition 分片 active-active**：看 conditional 量级是否撑到单机触发
  延迟抖动；如果到了，按 user_id hash 水平扩展

## 参考 (References)

- ADR-0031: Counter / Match HA cold standby rollout
- ADR-0036: Quote 引擎状态 snapshot + 热重启（offset 处理同款）
- ADR-0040: Conditional 服务整体设计
- ADR-0041: Counter reservations（触发链路的幂等依赖）
- 实现：[conditional/cmd/conditional/main.go](../../conditional/cmd/conditional/main.go)、[pkg/election/](../../pkg/election/)
