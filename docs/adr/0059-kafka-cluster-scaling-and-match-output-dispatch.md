# ADR-0059: Kafka 集群扩容策略与撮合输出分发模式（长期调研）

- 状态: Research（长期调研 / 未立项实施）
- 日期: 2026-04-22
- 决策者: xargin, Claude
- 相关 ADR: 0003（Counter-Match via Kafka）、0004（counter-journal）、0010（Counter sharding by user_id，已被 0058 取代）、0017（Kafka transactional.id）、0020（trade-event topic）、0048（Snapshot + offset 原子绑定）、0049（Snapshot protobuf/json）、0050（Match input topic per-symbol）、0055（Match as orderbook authority）、0057（Asset service + Transfer Saga）、0058（Counter 虚拟分片 + 实例锁）

> **重要声明**：本 ADR **不作为短期实施方案**，仅用于：
>
> 1. 记录"Kafka 单集群扛不住一线 CEX 量级"这一架构红线，避免未来把单集群当永久假设
> 2. 对比业界三种主流撮合输出分发模式（OpenTrade 当前 / Bybit / OKX 推测），沉淀决策依据
> 3. 定义"何时触发集群扩容 / 分发模式切换"的量化触发条件，供后续真正立项时引用
> 4. 附录 A：记录 Bybit "per-user snapshot + 懒加载" 模型的调研，供 OpenTrade 未来 snapshot 模型演进参考

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标 |
|---|---|---|
| `撮合输出分发模式` | Match 产出成交后，如何把事件传给 Counter（账户层）的整体机制 | — |
| `dual-emit` | 同一笔 Trade，Match 在一个 Kafka 事务内为 maker、taker 各产一条事件到各自 partition（OpenTrade ADR-0058 phase 4a 当前方案） | — |
| `broadcast-filter` | Match 只产一条事件到 symbol 维度 topic，所有 Counter 实例广播消费后本地过滤掉不属于自己 user shard 的事件 | Bybit 模式 |
| `cluster-replicated bus` | Match 以 Raft-replicated UDP stream 对外广播成交，Counter 订阅 stream 后本地过滤 | OKX 推测（Aeron Cluster 模式） |
| `crossIdx` | Bybit 术语，一个撮合引擎实例的索引，对应一组 symbol（按 coin + contract type 分组） | OpenTrade 目前无对应概念；最接近"一组相关 symbol" |
| `CrossReq / CrossResp topic` | Bybit 的 Trading ↔ Cross 通信 Kafka topic，每个 crossIdx 一对 | OpenTrade 的 `order-event-{symbol}` / `trade-event` 类比 |
| `linear / inverse` | Bybit 合约类型（linear=U 本位，inverse=币本位）；两类合约用物理隔离的 Kafka 集群 | OpenTrade 未区分（当前只有现货） |
| `Aeron Cluster` | LMAX 系开源的 Raft-based 集群通信库，基于 UDP unicast/multicast，延迟 μs 级 | — |
| `ingress / egress` | Aeron Cluster 的 session-based 点对点消息通道（client→leader / leader→client） | gRPC unary request/response 类比 |
| `publication stream` | Aeron 的一对多广播通道，所有订阅者都能看到同一份消息流 | Kafka topic 广播语义类比 |
| `per-shard snapshot` | 一个分片内所有 user 的账户状态打包成一份 snapshot 文件（OpenTrade ADR-0058 当前） | — |
| `per-user snapshot` | 每个 user 独立一份 snapshot 文件，分片 ID 作为目录分桶 | Bybit `PerCoinUserTradingData` |
| `active user / 活跃用户` | 在任一时刻有持仓 / 活动订单 / 待触发条件单的用户；启动时需要 preload | Bybit `PreLoadUserIds` |
| `lazy load / 懒加载` | 非活跃用户的 snapshot 在首次请求到达时才读取，避免冷启动加载全量用户 | — |
| `thread amplification / thread 放大` | Go runtime 中 goroutine 读阻塞式 IO（如 NFS）时，runtime 为保证其他 goroutine 能跑会创建新 OS thread；大量并发时可能触发 `MaxThreads` 上限 | — |

## 背景 (Context)

### 1. 单集群容量天花板

前置讨论（对话记录 2026-04-22）算过 OpenTrade 在头部 CEX 峰值场景的 Kafka 流量：

| 场景 | 估算入流量 | 备注 |
|---|---|---|
| 稳态 10k tps | ~9 MB/s | 单集群 3-5 broker NVMe 轻松扛 |
| 爆单 100k tps | ~90 MB/s | 单集群仍在舒适区 |
| 头部 CEX 150w+ tps | **~3-5 GB/s producer 入流量**（加 RF=3 replica 到 9-15 GB/s，加 consumer fanout 再放大 4-5x） | **单集群接近/超过业界公开运行上限** |

业界可公开查证的单集群上限数据：

- LinkedIn 官方工程 blog：最大 Kafka 逻辑集群约 4000 broker（但物理按业务拆分），单物理集群稳态 GB/s 量级
- Confluent 官方 benchmark：单 broker ~1 GB/s producer throughput（理想条件，生产环境打 50% 折扣）
- Netflix Keystone 公开数据：peak 17M msg/s，分散在 36+ 集群

**共识经验值**：单 Kafka 集群稳态 3-5 GB/s 入流量 是业界生产环境的实际上限；再往上运维复杂度指数上升。

### 2. ADR-0058 的 dual-emit 与多集群扩展的矛盾

ADR-0058 phase 4a（commit `e34ed12`）落地了 trade-event 的 dual-emit：Match 为同一笔 Trade 同事务产两条事件到 maker/taker 各自的 vshard partition。这一设计：

1. 强依赖 **Kafka 事务的跨 partition 原子性**（transactional.id = `counter-vshard-{id}-ep-{epoch}`）
2. Kafka 事务严格 cluster-scoped，**无法跨集群**
3. 任何按 `user shard` 或按 `symbol` 切集群的方案都会破坏该事务

这意味着 OpenTrade 目前的架构在扩到头部 CEX 量级时，**必须回答"怎么拆集群"这个问题**，而目前没有答案。

### 3. 触发本次调研的背景讨论

- 用户提出"partition 多了 Kafka 写性能会下降吧" → 澄清 KRaft 下 partition 多不是问题
- 用户提出"能把多 partition 改成多 topic 分不同集群吗" → 引出"按 user 拆会破 dual-emit 事务"
- 用户提出"一线交易所 150-200w tps，单集群扛得住吗" → 确认单集群扛不住，需要多集群
- 用户提出"Binance 多集群的话 dual-emit 怎么办" → 推出"按业务 / symbol / region 拆，不能按 user 拆"的红线
- 用户提出"下单回包按 user 拆，按 symbol 拆集群会破 TxnProducer" → 进一步压缩可行维度到"按 biz_line 拆"
- 用户提出"看看 Bybit leak 他们是不是按 symbol 拆回包的" → **发现 Bybit 走的是另一条路线：broadcast-filter，根本不需要 dual-emit 事务**
- 用户补问 OKX 用 Aeron Cluster 时怎么回包 → 推测出第三种路线：cluster-replicated bus

这三条路线各自的适用条件和代价值得系统化沉淀，即使现阶段不切换也要避免未来决策时重新摸索。

## 三种撮合输出分发模式对比

### 模式 A：OpenTrade 当前 —— dual-emit + user partition

```
Match (transactional producer)
  begin txn:
    produce trade-event  partition = maker_vshard
    produce trade-event  partition = taker_vshard
  commit txn                       ← Kafka 事务保证两条原子
      │
      ▼
Counter vshard-N (只订阅 partition N)
  dedup by (user, symbol, match_seq_id)
  写 counter-journal partition = N
```

**核心特征**：
- Match 承担 partition routing 责任（要算 `xxhash64(user_id) % 256`）
- 精确投递，每条事件只被"应该处理"的那个 Counter 消费
- 消费侧无过滤开销
- 依赖 Kafka 事务保证 maker/taker 两条消息原子

**瓶颈**：集群不可拆分 —— 按 user 拆破 dual-emit，按 symbol 拆破 TxnProducer（counter-journal 跟 user / order-event 跟 symbol 的跨集群事务不存在）

### 模式 B：Bybit broadcast-filter —— 按 symbol 拆 topic + 消费端过滤

基于 leak 代码验证（`/Users/xargin/bybit-leaked/trading/`）的实际架构：

```
Trading Service (Counter 角色, 按 (coin, user_shard) 拆实例)
  │
  │ produce CrossReq-{crossIdx}         ← 按 crossIdx 拆 topic
  ▼
Cross (Match 角色, 按 crossIdx 独立部署, 代码不在 leak 里)
  │
  │ produce CrossResp-{crossIdx}        ← 单写一条, 含 maker_uid + taker_uid
  ▼
所有 Trading 实例订阅 CrossResp-{crossIdx}
  for each xRespPkg:
    if shardingbyuid.CanProcess(maker_uid):
        处理 maker 侧
    if shardingbyuid.CanProcess(taker_uid):
        处理 taker 侧
    else:
        丢弃
```

**代码证据**（leak 路径为绝对路径）：

- [send_to_cross.go:87](/Users/xargin/bybit-leaked/trading/trading_service/tradingoutput/send_to_cross.go:87)：`xreqchv3.BuildAndSendCrossRequest(...)` 实际走 Kafka produce 到 CrossReq topic
- [xrespinput/receiver.go:22-45](/Users/xargin/bybit-leaked/trading/trading_service/tradinginput/xrespinput/receiver.go:22)：每个 crossIdx 有独立 `CrossRespTopicName()`，按 offset seek 消费
- [tradingdumprecovery/recovery.go:105](/Users/xargin/bybit-leaked/trading/trading_service/bootstrap/initialization/tradingrecovery/tradingdumprecovery/recovery.go:105)：`bkafka.MustGetKImpl(xcomm.GetMasterK(crossIdx))` —— 不同 crossIdx 用不同 Kafka 集群
- [dump_result.go:123-129](/Users/xargin/bybit-leaked/trading/trading_service/tradingoutput/dump_result.go:123)：`if symbolconfig.IsInverse(pkg.RespHeader.Coin)` —— linear/inverse 写不同 Kafka 集群（rkabc / rkt123）
- [bybit-trading-architecture-analysis.md §8.2](/Users/xargin/bybit-leaked/trading/bybit-trading-architecture-analysis.md)：每个 Trading 实例按 `userId % UserIdDivisor == UserIdRemainder` 判断是否该处理

**核心特征**：
- Match 只写自己独占的 topic（单写），不需要知道有几个 Counter shard
- 所有 Counter 实例都订阅这个 topic，本地按 `uid % N` 决定是否处理
- **没有 Kafka 事务依赖** —— maker/taker 分发责任从 producer 挪到 consumer
- Kafka 集群可以按 crossIdx / 产品线自由拆分

**代价**：
- **带宽放大**：每条 CrossResp 被所有 Trading 实例重复消费（假设每 coin 10 shard，放大 10x）
- **消费侧 CPU 开销**：每个实例丢弃 9/10 的消息
- **topic 数多**：每个 crossIdx 一对 req/resp topic，运维 topic 数 = O(symbol_groups)

### 模式 C：OKX cluster-replicated bus —— Aeron Cluster（推测）

**声明**：OKX 内部架构**没有公开泄露**。下列描述基于 Aeron Cluster 的通用机制 + CEX 领域必然约束推断，不是 OKX 官方公开事实。

```
Counter ──Aeron ingress (UDP unicast)──▶ Match Cluster Leader
                                            │
                                   [Raft log 复制到 follower]
                                            │
Counter ◀──Aeron egress (UDP unicast)──── Match Leader   ← 下单 ACK（session-targeted）
                                            │
                                            │ publication stream (UDP multicast)
                                            ▼
                            ┌──────────────┬─────────────┬─────────────┐
                            ▼              ▼             ▼             ▼
                  所有 Counter 实例     Quote / marketdata    Risk       trade-dump
                  各自本地过滤 user
```

**核心特征**：
- 下单请求走 ingress，ACK 走 egress（session 定向），天然点对点
- 成交广播走 publication stream，所有订阅者多播接收
- Raft 保证所有 Follower 看到的顺序完全一致 —— 替代 Kafka 事务的作用
- 延迟 μs 级（vs Kafka 几 ms）
- **消费端过滤**逻辑和 Bybit broadcast-filter 一样：所有 Counter 收到全量 trade，本地过滤

**代价**：
- 持久化需外挂（Aeron Cluster 不做长期 retention）：stream 要 bridge 到 Kafka / S3 供 trade-dump、history 查询
- Aeron Cluster 单集群规模有限（典型 3-5 节点 Raft），超过要按 symbol group 再拆多 cluster
- 运维门槛高：Aeron 生态比 Kafka 小得多，对团队技能要求高
- 调试困难：UDP 二进制流不像 Kafka 那样 `kafka-console-consumer` 一下就能看内容

### 三模式对比表

| 维度 | A: OpenTrade dual-emit | B: Bybit broadcast-filter | C: OKX cluster-replicated (推测) |
|---|---|---|---|
| Match 侧复杂度 | 高（partition routing + Kafka 事务） | 低（单写独占 topic） | 中（Raft 集群管理）|
| Counter 侧复杂度 | 低（只订自己 partition） | 中（订阅多 topic + 过滤） | 中（订阅 publication + 过滤） |
| 是否依赖 Kafka 事务 | **强依赖** | 无 | 无（Raft 替代） |
| 多 Kafka 集群扩展性 | 差（按 user/symbol 拆都会破事务） | **好**（按 crossIdx 天然分集群） | 好（Aeron Cluster 自带分组） |
| 带宽使用 | 精确投递（1x）| 放大（~N_shard x）| 放大（~N_consumer x）|
| 消费侧 CPU 过滤开销 | 无 | 中 | 中 |
| 单集群容量上限 | ~3-5 GB/s 后需拆 | 同上，但拆起来容易 | Aeron 单 Cluster ~百 MB/s，但易拆 |
| 端到端延迟 | ms 级 | ms 级 | **μs 级** |
| 运维成熟度 | 高（纯 Kafka）| 高（纯 Kafka，多 topic）| 低（Aeron 生态小）|
| 一致性模型 | 强（Kafka 事务）| 最终一致（seq 单调 + 消费端幂等）| 强（Raft） |
| topic / stream 数量 | 少（~3 核心 topic）| 多（O(symbol_groups) 对 req/resp）| 中（per cluster） |
| 故障 blast radius | 单集群挂=全停 | 按 crossIdx 隔离 | 按 Aeron Cluster 隔离 |

## 决策 (Decision)

**当前阶段：维持模式 A（ADR-0058 dual-emit），不做切换。**

**本 ADR 不立项实施**，仅作为：

1. **架构红线备忘**：Kafka 单集群稳态超过 3-5 GB/s 入流量需要启动拆分评估
2. **扩容路径选项库**：未来真碰到扩容瓶颈时，优先从模式 B / C 中选，不要再重新探索
3. **触发条件定义**：见下"触发条件"章节

## 何时触发切换（触发条件）

以下任一条件达成时，启动 ADR-0059 的正式立项评估（可能是实施某个模式，也可能是起草 ADR-0060 更新本决策）：

### 硬触发（必须启动评估）

- **Kafka 单集群稳态入流量 ≥ 3 GB/s**（含 RF 前，producer 侧）
- **单 broker 7 天 avg disk util ≥ 70%** 或 **网络带宽 util ≥ 70%**
- **Kafka controller metadata log 推送 p99 ≥ 500ms**（表示元数据层成瓶颈）
- **任何 rolling restart 单 broker 耗时 ≥ 15 分钟**

### 软触发（建议启动评估）

- OpenTrade 即将引入第二个 biz_line（futures / perpetual / options），**按产品线拆集群** 是最自然的切入点
- 峰值订单 tps 稳态 ≥ 10w，业务增长曲线预计半年内触及硬触发
- 团队开始讨论"Kafka 要不要换 Pulsar / Redpanda / 自研"之类的话题，本 ADR 的三模式对比可以先于技术选型给出结构化思考框架

## 未来走哪条路的初步倾向

**不是决策，只是倾向性记录**，真要走时按届时实际情况重新评估：

- **近期扩容（按产品线拆）**：spot / futures / options 各自一个 Kafka 集群，**单集群内部维持模式 A**（dual-emit 不动）。这是成本最低的扩容方式，2-3 个集群能撑到 50-100w tps。
- **中期扩容（按 symbol group 进一步细拆）**：单产品线内再按 symbol group 拆集群时，**必须切换到模式 B（broadcast-filter）**，因为模式 A 不支持。切换成本：Match 侧重写分发逻辑（去掉 partition routing + Kafka 事务），Counter 侧加本地过滤。
- **长期（极致低延迟 / 头部 CEX 体量）**：评估模式 C（Aeron Cluster），但前提是团队具备 Aeron 运维能力，且业务确实卡在延迟上（ms→μs 的改进值得复杂度代价）。

**禁止的扩容方式**（避免未来走弯路）：

- ❌ 按 user shard 拆 Kafka 集群（破 dual-emit）
- ❌ 在模式 A 下按 symbol 拆 Kafka 集群（破 Counter TxnProducer）
- ❌ 把 256 partition 改 256 topic 但留在同集群（徒增 metadata 开销，无收益）

## 备选方案 (Alternatives Considered)

### 备选 1：立即切换到模式 B，提前布局

**拒绝理由**：
- OpenTrade 未上线，当前 scale 需求远未触及单集群瓶颈
- 模式 B 的 topic 数 / 消费侧过滤开销 / 一致性语义调整都有实实在在的成本
- 提前优化 = 过度设计，违背"breaking change 无需顾虑的上线前窗口应该用来把核心路径打磨简单而非复杂化"的原则

### 备选 2：废弃 ADR-0058 dual-emit，直接用模式 B

**拒绝理由**：
- ADR-0058 phase 4a 刚 commit（`e34ed12`），phase 4b 还在 roadmap 上
- dual-emit 在单集群 scale 下是**更简单**的方案（消费侧零过滤开销，一致性强）
- 模式 B 的价值只在多集群场景才显现，单集群下纯粹劣化

### 备选 3：激进方向 —— 撮合核心迁 Aeron Cluster（模式 C）

**拒绝理由**：
- 团队当前无 Aeron 经验，运维风险高
- 延迟 μs 级的需求对现货 CEX 并不强烈（ms 级已经远快于撮合本身的决策延迟）
- 持久化外挂 Aeron→Kafka bridge 增加架构面

### 备选 4：完全放弃 Kafka，换 Pulsar / Redpanda

**拒绝理由**：
- 本 ADR 的讨论不是 Kafka 本身的问题，是"多集群架构下撮合输出分发模式"的问题；换 broker 实现不解决这个问题
- Pulsar 的跨集群复制能力看似相关，但 tenant/namespace 模型和交易所强一致需求匹配度不高
- 成熟度 / 运维生态 / 团队熟悉度 Kafka 仍占优

## 理由 (Rationale)

### 为什么写成"长期调研" ADR 而非直接立项

1. **避免重复摸索**：下一次团队（或我自己）再讨论"怎么拆 Kafka 集群"时，不用再从 partition vs topic 讨论到 dual-emit 事务悖论再推出三种模式 —— 本 ADR 直接给出结论
2. **触发条件明确**：拆集群不是想做就做，需要量化信号。在没到信号前写实施方案是白费
3. **架构红线**：把"禁止按 user shard 拆 Kafka 集群"这条写死在 ADR 里，未来有人提"按用户拆"时可以直接引用本 ADR 否决

### 为什么把 Bybit / OKX 两种模式都记下来

- Bybit 有 leak 代码作为硬证据（路径见下文"参考"章节），可以直接验证架构细节
- OKX 基于 Aeron Cluster 是业界广泛讨论的技术路线，即使具体实现没泄露，Aeron 本身的通信原语是公开的，推断可信度足够作为"未来选项之一"
- 两者覆盖了"Kafka 生态内扩展"和"跳出 Kafka"两类方向，对未来决策形成完整对比

### 为什么不立即切 Bybit 模式

- 单集群下模式 A 的"精确投递 + 强一致"在可观测性、调试、一致性推理上都更简单
- 模式 B 的核心优势只在**多集群**场景才显现，单集群下纯粹是放大 + 过滤的开销
- OpenTrade 上线到触及 3 GB/s 单集群瓶颈之间还有很长时间窗口，届时再决定更稳

## 影响 (Consequences)

### 正面

- 三种撮合输出分发模式的 trade-off 被结构化记录，未来决策有参考
- "不能按 user shard 拆集群"的红线明文化，避免误判
- 触发条件量化，避免"什么时候该拆"这类模糊讨论

### 负面 / 代价

- 本 ADR 不产生代码改动，短期无直接价值 —— 属于"延迟满足"型投资
- 三模式对比可能随 Kafka / Aeron 生态演进而过时，需要定期 review（建议每 12 个月回顾一次）

### 中性

- 维持 ADR-0058 的 dual-emit 作为当前实现，符合"breaking change 窗口优先把核心路径做简单"的原则
- Bybit 代码 leak 路径写入本 ADR，未来如果 leak 源被清理，引用可能失效（本地备份保留）

## 实施约束 (Implementation Notes)

本 ADR 不立项实施，**只需要在以下时点触发 review**：

1. 定期：每 12 个月由架构负责人 review 一次，检查触发条件是否接近
2. 事件驱动：
   - 引入新 biz_line（futures / options 等）前
   - Kafka broker 监控指标触及硬触发阈值时
   - 团队讨论"Kafka 要换掉"相关话题时
   - 有 ADR 提出"拆 Kafka 集群"方案时

监控指标建议（主要给 SRE 参考，和本 ADR 触发条件对齐）：

```
kafka_server_brokertopicmetrics_bytesinpersec        # 稳态入流量
kafka_log_logflushstats_logflushrateandtimems_p99    # flush p99
kafka_server_requestmetrics_totaltimems_p99           # 端到端请求耗时
node_disk_io_time_seconds_total                      # disk util
node_network_transmit_bytes_total                    # 网络带宽
kafka_controller_metadata_log_publish_time_p99       # controller 元数据推送
```

## 参考 (References)

### 项目内

- ADR-0003：Counter ↔ Match 异步 Kafka 通信（奠定当前架构）
- ADR-0010：Counter 固定 10 shard（已被 ADR-0058 取代）
- ADR-0017：Kafka transactional.id 的 fencing 机制
- ADR-0020：trade-event topic 定义
- ADR-0050：Match input topic per-symbol
- ADR-0055：Match as orderbook authority（Bybit 风格）
- ADR-0057：Asset service + Transfer Saga
- ADR-0058：Counter 虚拟分片 + 实例锁 + 动态迁移（当前 dual-emit 方案所在）

### Bybit leak 代码证据（本地路径）

撮合输出分发模式（模式 B）相关：

- `/Users/xargin/bybit-leaked/trading/trading_service/tradingoutput/send_to_cross.go` —— `SendToCross` 走 Kafka produce
- `/Users/xargin/bybit-leaked/trading/trading_service/tradingoutput/dump_result.go` —— linear/inverse 写不同 Kafka 集群（`rkabc` / `rkt123`）
- `/Users/xargin/bybit-leaked/trading/trading_service/tradinginput/xrespinput/receiver.go` —— Trading 订阅 CrossResp topic 处理成交回报
- `/Users/xargin/bybit-leaked/trading/trading_service/bootstrap/initialization/tradingrecovery/tradingdumprecovery/recovery.go` —— 按 crossIdx 确定 Kafka 集群归属
- `/Users/xargin/bybit-leaked/trading/bybit-trading-architecture-analysis.md` §8 —— 分片配置 etcd 结构（`trading_sharding_config.{COIN}` + `userId % Divisor == Remainder`）

附录 A（per-user snapshot + 懒加载）相关：

- `/Users/xargin/bybit-leaked/trading/trading_service/cmd/main.go:44-47` —— **fixme 注释**：EFS goroutine thread 放大问题
- `/Users/xargin/bybit-leaked/trading/trading_service/tools/reset_maker_fee_rate/main.go:92-110` —— 三级目录分桶 + per-user `.pb` 文件的实际布局
- `/Users/xargin/bybit-leaked/trading/trading_service/bootstrap/initialization/tradingrecovery/tradingdumprecovery/get_shard_recovery_dump.go` —— `tradingdumpsdk.GetShardRecoveryDump` 调用
- `/Users/xargin/bybit-leaked/trading/trading_service/internal/dispatcher/tradingmod/preworker/load_trading_dump.go:47,87` —— 懒加载与同步加载路径，`tradingdumpsdk.LoadCoinStore(userId, coin, mustLoad)`
- `/Users/xargin/bybit-leaked/trading/bybit-trading-architecture-analysis.md` §4.3 —— EFS 存储 + Protobuf 格式 + 活跃用户定义

### 外部公开资料（用于支持"单集群上限"和"Aeron Cluster"相关推断）

- Confluent 官方文档：`How to choose the number of partitions` / `Kafka Performance Benchmarks`
- Apache Kafka KIP-500（KRaft 模式，取代 ZooKeeper 的元数据层）
- LinkedIn Engineering Blog：`Kafka at LinkedIn` 系列
- Aeron 官方文档：`Aeron Cluster` / `Aeron Archive`（Raft + UDP publication 的通信原语）

### 讨论

- 2026-04-22：用户与 Claude 关于"单集群扛不住 150-200w tps / Bybit 如何拆"的对话（会话记录）

## 附录 A：Snapshot 粒度与懒加载调研（per-user vs per-shard）

本附录记录 Bybit "per-user snapshot + 懒加载" 模型的调研结论，**作为 OpenTrade snapshot 模型（ADR-0048 / 0058）未来演进的候选方向之一**，不立即实施。

### A.1 背景

调研发现于 2026-04-22 的会话：追问 Bybit 柜台（Trading Service）的快照由谁打，意外挖出两个关键发现：

1. **Bybit 的 snapshot 粒度是 per-user**（`PerCoinUserTradingData`），而不是 per-shard
2. **Bybit 实际踩过"Go goroutine 读 EFS block 导致 thread 爆炸"的坑**，代码里有 fixme 注释

这两点对 OpenTrade 当前的 per-vshard snapshot 模型（ADR-0058）有直接启示意义。

### A.2 Bybit 实际方案（代码证据）

#### A.2.1 独立 dump service 负责打快照，Trading 只读

- `tradingdumpsdk.GetShardRecoveryDump(coin, shardName)` — 返回 `PerCoinShardRecoveryDump`（含活跃用户列表 + 下次消费 offset）
- `tradingdumpsdk.LoadCoinStore(userId, coin, mustLoad)` — 加载单个 user 的完整账户快照
- SDK 背后的 dump service **代码不在 leak 里**（独立服务）；从调用面推断：消费 `trading_result` topic 按 (coin, userId) 聚合并周期性写 EFS

Trading 本身**只读不写** snapshot，是真正的无状态服务（状态在内存里都是 dump 加载 + Kafka replay 出来的）。

#### A.2.2 文件布局：三级分桶 + per-user 一个 .pb 文件

[tools/reset_maker_fee_rate/main.go:92-110](/Users/xargin/bybit-leaked/trading/trading_service/tools/reset_maker_fee_rate/main.go:92) 的工具代码直接展示了实际文件布局：

```go
path := fmt.Sprintf("%s%s_user_data/%d/", BackDumpBath, coin, processShard)
for j := 0; j < 10; j++ {
    p := fmt.Sprintf("%s%d/%d/", path, shard, j)
    fileInfos, err := ioutil.ReadDir(p)
    for _, f := range fileInfos {
        filepath := fmt.Sprintf("%s%s", p, f.Name())
        fileBytes, _ := ioutil.ReadFile(filepath)
        userData := &tradingdump.PerCoinUserTradingData{}
        proto.Unmarshal(fileBytes, userData)
    }
}
```

```
{EFS_root}/BTC_user_data/
  {processShard 0-9}/      ← 第一级桶
    {shard 0-9}/           ← 第二级桶
      {j 0-9}/             ← 第三级桶
        {userId}.pb        ← per-user 一个文件
```

三级分桶 = **1000 个叶子目录**，每目录约 1000 文件（百万用户量级）。目的是避免单目录 `readdir` 性能崩溃。

#### A.2.3 活跃 / 非活跃分层加载

- **活跃用户**：启动时从 `PreLoadUserIds` 预加载（有持仓 / 活动单 / 条件单的用户，估计量级从千万用户里筛到几十万）
- **非活跃用户**：懒加载，首次请求到达时才 `LoadCoinStore(userId)`（[preworker/load_trading_dump.go:47](/Users/xargin/bybit-leaked/trading/trading_service/internal/dispatcher/tradingmod/preworker/load_trading_dump.go:47)）

懒加载对"千万用户但 99% 非活跃"的 CEX 用户模型是**巨大的冷启动优化** —— 省掉 99% 的无用 IO。

#### A.2.4 Bybit 踩过的性能坑（fixme 铁证）

[trading_service/cmd/main.go:44-47](/Users/xargin/bybit-leaked/trading/trading_service/cmd/main.go:44)：

```go
func start() {
    // fixme: 进程重启时由于读EFS的goroutine陷入系统调用，导致不停的创建thread
    // 默认值是1W，先调整下限制，避免进程crash的风险
    debug.SetMaxThreads(10000 * 2)
```

**根因链**：

1. 每个 goroutine 读 EFS 走 `syscall.Read`，在 NFS 协议下会 block（等网络 RPC）
2. Go runtime 检测到 M（OS thread）被 block 在 syscall，为让其他 goroutine 能跑，**创建新 M**
3. 冷启动并发读几万个 user 文件 → runtime 瞬间创建几万个 OS thread
4. Go runtime 默认 `MaxThreads = 10000`，超限直接 `panic: runtime: thread limit exceeded`
5. 临时方案：`debug.SetMaxThreads(20000)` 翻倍。fixme 注释表明根本问题没解决

这是"大量并发 goroutine + 慢阻塞 IO"在 Go runtime 下的**典型陷阱**，任何走这条路线的项目都会撞到。

### A.3 per-user vs per-shard snapshot 对比

| 维度 | per-shard（OpenTrade ADR-0058 当前） | per-user（Bybit） |
|---|---|---|
| 文件数量 | O(shard 数) — 256 份 | O(用户数) — 千万级，分桶到 1000 目录 |
| 单文件大小 | 100 MB 级（一个 vshard 全部 user） | 2-20 KB 级（单 user 账户状态） |
| 冷启动 IO 次数 | 少（256 次读）| 多（活跃用户量级，10w-100w 次读）|
| 冷启动时间 | 受单文件大小 + 解析速度限制（秒级）| 受并发 IO + 文件系统元数据性能限制（10-30s 量级，且有 thread 爆炸风险）|
| 懒加载支持 | ❌ 不支持（必须整 shard load）| ✅ 支持（非活跃用户延迟到运行时）|
| 单 user 热迁移 | ❌ 只能整 vshard 迁 | ✅ 原生支持（直接把该 user 的 .pb 交给新实例）|
| 热点用户隔离 | 粒度粗（1/256 流量）| 粒度细（单用户级别）|
| snapshot 与 offset 原子性 | ADR-0048 方案已成熟 | 需要额外协议保证 per-user 快照和 Kafka offset 对齐 |
| 存储介质要求 | 对 S3/NFS/本地盘都友好 | **强烈依赖 NFS（EFS/FSx/Ceph）**，S3 会让冷启动慢 10x |
| Go runtime 调优 | 不需要 | **必须调**（thread 上限 + IO 模型）|
| dump 生成位置 | Counter 自己打（vshard owner 职责） | 独立 dump service 打 |

**核心 trade-off**：per-shard 是"加载快、粒度粗、运维简单"；per-user 是"灵活、懒加载、但配套工程成本高"。

### A.4 OpenTrade 何时考虑切向 per-user snapshot

**硬触发**（任一条件达成启动评估）：

- 单 vshard snapshot 大小 ≥ 500 MB（加载时间超 10s，影响故障 recovery SLO）
- 出现"极端热点用户"无法通过 vshard 粒度隔离（例如单个做市商占用 1 个 vshard 80% 资源，整 vshard 迁走成本高）
- 冷启动时间（从进程 start 到 ready）≥ 30s 且无法通过 snapshot 优化进一步压缩
- 用户规模增长到百万级活跃 + 千万级总量，"加载全部 user" 的内存占用 ≥ 单机内存 50%

**软触发**（建议同步评估）：

- 业务要求"分钟级 recovery SLO"，per-shard 加载时间不够用
- 引入"冷热账户分层"业务需求（例如资金账户、staking 账户等低频访问账户可以懒加载）

### A.5 切换时的配套工程要求（避免踩 Bybit 踩过的坑）

如果未来决定切 per-user snapshot，以下每一条都是**必须做**而非可选优化：

#### A.5.1 存储介质不能用 S3

- S3 单请求 20-100ms（HTTP + TLS + auth 固定开销），百万文件冷启动会慢到不可接受
- 必须用 NFS 协议的共享文件系统：**EFS / FSx for Lustre / 自建 Ceph / GlusterFS**
- 单机本地盘在 HA 场景下不行（实例 failover 后新实例访问不到）

#### A.5.2 Go runtime 必须调优

至少以下三选一（推荐多管齐下）：

1. **限制并发 IO goroutine 数**：用 `semaphore.Weighted` 或固定大小 worker pool 控制同时 read 的 goroutine 数（例如 ≤ 500）
2. **`debug.SetMaxThreads(N)`**：上限提前调大（例如 50000），但要配合 `ulimit -u` 检查
3. **用 `io_uring` 或 `aio` 替代阻塞 syscall**：Linux only，运维复杂度高，但能根治 thread 放大
4. **监控告警**：`/proc/<pid>/status` 的 `Threads:` 字段超过 5000 立即报警

#### A.5.3 必须做分桶目录

单目录文件数上限：

- EFS / NFS：几万文件以后 `readdir` 开始明显变慢，十万以上会崩
- 三级分桶（Bybit 方案）覆盖千万用户 → 每目录几千文件，运维可接受

分桶函数建议：`buckets = [xxhash64(userId) >> 48 & 0xff, ... 三级]`（避免和 vshard 哈希冲突）

#### A.5.4 snapshot 与 Kafka offset 原子性

继承 ADR-0048 的"snapshot 含 offset"原则，但要适配 per-user 粒度：

- 选项 A：shard 级 `PerCoinShardRecoveryDump`（含 offset）+ per-user data 文件，两类文件写入时序要设计（先 per-user 后 shard dump，类似 commit marker 模式）
- 选项 B：per-user 文件里内嵌 per-user `last_applied_seq_id`（Bybit 实际做法），shard 级 dump 记 Kafka offset，recovery 时用 `max(seq per user) vs shard offset` 验证一致性

#### A.5.5 Snapshot pipeline 的归属：与 trade-dump 共享基础设施

**核心观察**：Snapshot pipeline 的骨架"消费 Kafka → 按 user 聚合 → 周期性持久化"和 trade-dump 现有的 MySQL 投影 pipeline **是同构的** —— 只是 sink 从 MySQL 换成共享文件系统（EFS / FSx / Ceph），存储格式从 SQL 行换成 protobuf 文件。**不应当把它做成一个完全独立的新服务**。

##### A.5.5.1 "合并"在三个层次上含义不同，必须分清

| 层次 | 合并含义 | 建议 |
|---|---|---|
| **代码 / 基础设施层** | 共享 Kafka consumer 框架、proto schema、幂等 dedup、offset 管理、监控 / tracing | ✅ **必须共用**，trade-dump 已有的能力直接复用 |
| **进程 / 部署层** | 一个 binary 同时跑 MySQL 投影和 snapshot 两个 pipeline；或者同 binary 拆进程 | ⚠️ **初期同进程、后期可分进程**（按需演进）|
| **职责 / 一致性语义层** | 共用一个 consumer group、一份 offset、"一次消费双写"到 MySQL + 文件系统 | ❌ **严禁**（见 A.5.5.3）|

##### A.5.5.2 推荐架构：同 repo + 同 binary + 双独立 pipeline

```
┌─────────────────────────────────────────────────────┐
│                trade-dump binary                     │
│                                                      │
│  ┌────────────────────┐  ┌────────────────────────┐  │
│  │ MySQL pipeline     │  │ Snapshot pipeline      │  │
│  │ - group: td-sql    │  │ - group: td-snap       │  │
│  │ - offset 独立      │  │ - offset 独立          │  │
│  │ - sink: MySQL      │  │ - sink: EFS / FSx      │  │
│  │ - format: SQL rows │  │ - format: per-user .pb │  │
│  └────────────────────┘  └────────────────────────┘  │
│            │                       │                 │
│            └────── 共享基础设施 ─────┘                 │
│        (Kafka consumer SDK, proto, metrics,          │
│         tracing, dedup, lifecycle)                   │
└──────────────────────────────────────────────────────┘
                       │
   启动参数可选:
     --pipelines=sql         (只跑 MySQL 投影)
     --pipelines=snap        (只跑 snapshot)
     --pipelines=sql,snap    (两个都跑，同进程)
```

代码组织：

```
trade-dump/
  cmd/trade-dump/main.go       ← 按 --pipelines flag 加载对应 pipeline
  internal/
    consumer/                  ← 共享的 Kafka consumer 框架 (现有)
    mysql/                     ← MySQL sink (现有)
    snapshot/                  ← 新增：文件系统 sink + per-user 聚合
    shared/                    ← 幂等 / dedup / offset / metrics 等共享能力
```

##### A.5.5.3 硬约束：独立 consumer group、独立 offset、独立故障域

**绝对不可**走"一次消费双写到 MySQL + 文件系统"的路线。原因：

1. **故障耦合**：MySQL 挂了会卡住 snapshot 推进，反之亦然。两套存储的可用性特征完全不同（MySQL 主从切换 vs EFS mount 抖动），必须各自有 back-pressure 和 retry 策略。
2. **一致性语义不同**：
   - MySQL pipeline：offset commit 在 MySQL write 后（ADR-0023 的"MySQL-first"）
   - Snapshot pipeline：offset 记在 shard 索引文件内（A.5.4 的选项 A/B），Counter recovery 时 replay 补齐
   - "一次消费双写"无法同时满足两套一致性要求
3. **资源画像不同**：
   - MySQL pipeline：主要消耗 MySQL 连接池 + 网络
   - Snapshot pipeline：主要消耗文件系统句柄 + Go thread（A.5.2 的陷阱）
   - 资源隔离失败时，snapshot 的 thread 爆炸会直接打爆 MySQL pipeline
4. **迭代节奏不同**：MySQL schema 变更和 snapshot proto 演化的发布窗口对不齐，共享 offset 会互相牵制发布。

**所以**：必须是**两个独立 consumer group、两份独立 offset、两套独立告警**。共享的只是"怎么把一条 Kafka 消息拿到内存"这件事的脚手架。

##### A.5.5.4 何时从"同进程双 pipeline"升级为"分进程"

保留灵活性，以下任一条件出现时**再**拆进程：

- Snapshot pipeline 吞吐 / lag 需要与 MySQL pipeline 独立 scale（例如 snapshot 需要比 MySQL 更激进的并发）
- Snapshot pipeline 的故障（例如 thread 爆炸）频繁影响 MySQL pipeline SLO
- Snapshot pipeline 的发布节奏显著快于 / 慢于 MySQL pipeline
- 团队分工明确到"有一个人 / 小组专门负责 snapshot"

在此之前，**同进程两个 pipeline 是更经济的选择**（少一套部署 / 监控 / 告警）。拆的时候因为代码本身已经分包，改动仅限于部署配置，不碰业务逻辑。

##### A.5.5.5 为什么不学 Bybit 拆独立 trading dump service

Bybit 确实把 trading dump 做成独立服务，但他们的团队规模（数百工程师）和业务体量（全球头部 CEX）支撑得起多服务拆分的运维成本。OpenTrade 初期团队规模下，**多一个服务意味着多一套 CI / 部署 / 监控 / 值班**，边际收益远不如"在 trade-dump 内加一个 pipeline"。

基础设施层面 Bybit 也是**共用**的（`golib/mod/bizmsgch/tradingresultch`、`golib/mod/tradingdumpsdk`、`golib/pkg/bkafka` 在不同 binary 间通过 Go module 共享），不是每个服务都重写一遍。这和本附录推荐的"trade-dump 内部模块共享 + 部署按需拆分"方向一致，只是颗粒度不同。

### A.6 为什么不立即切

1. **OpenTrade 现阶段用户规模远不到 per-shard 的天花板**：256 vshard 每个最多几万用户，snapshot 百 MB 级，加载 1-2 秒
2. **配套成本高**：存储介质升级（S3 → EFS 或 Ceph）、Go runtime 调优、trade-dump 增加 snapshot pipeline，都是工程投入
3. **per-user 真正的杀手锏是懒加载**，而 OpenTrade 当前用户模型下"活跃/非活跃"差距还不显著（上线初期所有用户基本都是活跃的）

### A.7 初步倾向（不构成决策）

- **OpenTrade 用户规模达到 100 万活跃 + 1000 万总量之前**：维持 ADR-0058 的 per-vshard snapshot，不切
- **达到上述规模 或 触发 A.4 硬触发条件时**：切 per-user snapshot，**在 trade-dump 内新增 snapshot pipeline**（A.5.5），共享 Kafka consumer / proto / monitoring 基础设施，初期与 MySQL pipeline 同进程部署
- **存储介质**：文件系统（EFS / FSx / Ceph），不用 S3（A.5.1）
- **切换时严格对齐 Bybit 踩过的坑清单（A.5）**，不要重走弯路



- [ ] 每 12 个月回顾一次本 ADR 的三模式对比，补充新的业界参考（如 Binance / Coinbase 公开技术博客更新）
- [ ] Kafka 生产监控面板接入触发条件章节的指标
- [ ] 引入 futures / perpetual 等新 biz_line 时，先对照本 ADR 的"按产品线拆"路径评估
- [ ] 如果团队有 Aeron 实践经验引入，补充模式 C 的可行性细化
- [ ] 如果 Bybit leak 源被清理，把本地路径改为快照存档路径
- [ ] 监控 vshard snapshot 大小，接近 500 MB 时启动附录 A 的 per-user snapshot 评估
- [ ] 监控冷启动耗时（进程 start → ready），超过 30s 时评估是否需要切 per-user 懒加载
- [ ] 对 Go runtime `runtime.NumThread`（或 `/proc/<pid>/status` 的 `Threads:`）加监控告警，为未来 per-user snapshot 切换做准备
