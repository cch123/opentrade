# ADR-0059: Kafka 集群扩容策略与撮合输出分发模式（长期调研）

- 状态: Research（长期调研 / 未立项实施）
- 日期: 2026-04-22
- 决策者: xargin, Claude
- 相关 ADR: 0003（Counter-Match via Kafka）、0004（counter-journal）、0010（Counter sharding by user_id，已被 0058 取代）、0017（Kafka transactional.id）、0020（trade-event topic）、0050（Match input topic per-symbol）、0055（Match as orderbook authority）、0057（Asset service + Transfer Saga）、0058（Counter 虚拟分片 + 实例锁）

> **重要声明**：本 ADR **不作为短期实施方案**，仅用于：
>
> 1. 记录"Kafka 单集群扛不住一线 CEX 量级"这一架构红线，避免未来把单集群当永久假设
> 2. 对比业界三种主流撮合输出分发模式（OpenTrade 当前 / Bybit / OKX 推测），沉淀决策依据
> 3. 定义"何时触发集群扩容 / 分发模式切换"的量化触发条件，供后续真正立项时引用

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

- `/Users/xargin/bybit-leaked/trading/trading_service/tradingoutput/send_to_cross.go` —— `SendToCross` 走 Kafka produce
- `/Users/xargin/bybit-leaked/trading/trading_service/tradingoutput/dump_result.go` —— linear/inverse 写不同 Kafka 集群（`rkabc` / `rkt123`）
- `/Users/xargin/bybit-leaked/trading/trading_service/tradinginput/xrespinput/receiver.go` —— Trading 订阅 CrossResp topic 处理成交回报
- `/Users/xargin/bybit-leaked/trading/trading_service/bootstrap/initialization/tradingrecovery/tradingdumprecovery/recovery.go` —— 按 crossIdx 确定 Kafka 集群归属
- `/Users/xargin/bybit-leaked/trading/bybit-trading-architecture-analysis.md` §8 —— 分片配置 etcd 结构（`trading_sharding_config.{COIN}` + `userId % Divisor == Remainder`）

### 外部公开资料（用于支持"单集群上限"和"Aeron Cluster"相关推断）

- Confluent 官方文档：`How to choose the number of partitions` / `Kafka Performance Benchmarks`
- Apache Kafka KIP-500（KRaft 模式，取代 ZooKeeper 的元数据层）
- LinkedIn Engineering Blog：`Kafka at LinkedIn` 系列
- Aeron 官方文档：`Aeron Cluster` / `Aeron Archive`（Raft + UDP publication 的通信原语）

### 讨论

- 2026-04-22：用户与 Claude 关于"单集群扛不住 150-200w tps / Bybit 如何拆"的对话（会话记录）

## 后续工作建议（不构成本 ADR 决策，仅作为未来 reviewer 的 checklist）

- [ ] 每 12 个月回顾一次本 ADR 的三模式对比，补充新的业界参考（如 Binance / Coinbase 公开技术博客更新）
- [ ] Kafka 生产监控面板接入触发条件章节的指标
- [ ] 引入 futures / perpetual 等新 biz_line 时，先对照本 ADR 的"按产品线拆"路径评估
- [ ] 如果团队有 Aeron 实践经验引入，补充模式 C 的可行性细化
- [ ] 如果 Bybit leak 源被清理，把本地路径改为快照存档路径
