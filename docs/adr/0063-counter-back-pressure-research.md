# ADR-0063: Counter 反压机制（长期调研）

- 状态: Research（长期调研 / 未立项实施）
- 日期: 2026-04-22
- 决策者: xargin, Claude
- 相关 ADR: 0003（Counter-Match via Kafka 异步解耦）、0017（Kafka transactional.id）、0018（per-user sequencer）、0037（BFF 速率限制）、0057（Asset service + Transfer Saga）、0058（Counter 虚拟分片 + 实例锁）、0060（Counter 消费异步化 + TECheckpoint，提供反压所需指标）、0062（订单终态 evict，评估时需豁免）

> **重要声明**：本 ADR **不作为短期实施方案**，仅用于：
>
> 1. 记录"Counter 过载时需要反压"这个系统性问题
> 2. 梳理反压机制的设计空间、候选方案、各方案的 trade-off
> 3. 定义立项实施的触发条件
> 4. 避免未来做反压时从头讨论一遍已经想清楚的部分
>
> 立项时机未定 —— 需要观察 ADR-0060 上线后的真实负载画像才能决定关键参数。

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标 |
|---|---|---|
| `反压 / back-pressure` | 下游过载时对上游施加减速信号，防止无限堆积 | Reactive Streams、Kafka Streams 的 `max.task.idle.ms` |
| `反压作用面` | 反压在架构中的作用层（入口 / 消费 / 执行） | — |
| `反压等级` | 反压强度的离散分级（normal / yellow / red / critical 等） | — |
| `hysteresis / 滞后` | 升级和降级使用不同阈值，防止抖动 | 硬件控制系统术语 |
| `豁免列表` | 即使反压 active 也必须通过的请求类型（CancelOrder / admin / healthcheck 等） | — |
| `detector goroutine` | 周期采集指标、计算反压等级、发信号的独立协程 | — |

## 背景 (Context)

### Counter 过载的场景

Counter 处理速度可能低于上游投递速度的几种情况：

1. **做市商集中高频下单**：单 user 的 per-user queue 积压（ADR-0060 改无界链表后不会溢出，但内存涨）
2. **整体 trade-event 流量尖峰**：pendingList 深度增长，advancer 跟不上
3. **Kafka Broker 延迟恶化**：`TxnProducer.Publish` RTT 上升，所有 per-user drain 变慢
4. **GC 压力 / CPU 打满**：Counter 进程本身算力不够
5. **大批量 evict**（ADR-0062）：evictor 瞬时占用 per-user sequencer

### 当前没有反压的后果

| 场景 | 当前行为 |
|---|---|
| per-user queue 积压 | 无限链表，内存上涨直到 OOM |
| pendingList 积压 | 无上限，advancer lag 扩大，watermark 冻结 |
| Kafka Publish 慢 | 所有 fn 排队在 TxnProducer.mu 下，延迟爆炸 |
| 单节点 CPU 打满 | 继续接受新请求，p99 持续恶化 |

### ADR-0060 提供的兜底不等于反压

ADR-0060 的 `Publish 5s panic` 是 **fail-fast 熔断**，不是反压：
- 熔断：故障时整进程崩溃，触发 vshard 冷切迁移
- 反压：过载时主动减速，避免崩溃

两者互补，但反压应该在熔断之前生效。

### 为什么现在不立项实施

- ADR-0060 的消费模型还没上线，真实负载画像未知
- 反压阈值（如 pendingList 超过多少算 yellow）无法凭空设计，必须基于生产基准
- BFF / asset-service 侧的反压响应策略需要这些服务配合讨论，依赖多方对齐
- 用户自己也还没想清楚（MEMORY: 不盲目立项）

## 设计空间 (Design Space)

### 1. 反压作用面

```
请求流向:
  ┌─────────────────────┐
  │ 面 A: gRPC 入口      │ ← PlaceOrder / Transfer / CancelOrder
  │   (BFF / asset →     │
  │    Counter)          │
  └──────────┬──────────┘
             ▼
  ┌─────────────────────┐
  │ 面 B: Kafka 消费循环 │ ← trade-event 消费
  │   (Counter ← Match)  │
  └──────────┬──────────┘
             ▼
  ┌─────────────────────┐
  │ 面 C: 执行层         │ ← per-user drain / Publish
  │   (sequencer /       │
  │    TxnProducer)      │
  └─────────────────────┘
```

| 面 | 反压手段 | 副作用 | 可行性 |
|---|---|---|---|
| A (gRPC 入口) | 返回 429 / `ResourceExhausted` | 客户端感知, 需 BFF / asset 配合 backoff | ✅ 可行 |
| B (消费循环) | 降低 poll 频率 / 暂停 poll | Kafka lag 上涨, 不影响 Match | ✅ 可行 |
| C (执行层) | 不能反压（正在执行的 fn 不能中断） | 破坏事务原子性 | ❌ 不做 |

**共识**：反压只作用于 A + B，不触碰 C。

### 2. 候选触发指标

| 指标 | 采集源 | 反映的压力 | 采集代价 | 备注 |
|---|---|---|---|---|
| pendingList 深度 | VShardWorker（ADR-0060）| 整体消费积压 | 极低（atomic counter）| 最直接 |
| per-user queue 最大深度 | sequencer（ADR-0060）| 单 user 热点 | 低（遍历 sync.Map）| 小心遍历成本 |
| advancer lag | ADR-0060 的 advancer | watermark 推进滞后 | 极低 | 和 pendingList 高度相关 |
| TxnProducer Publish p99 | TxnProducer metrics | Kafka 健康度 | 低（histogram）| 外部依赖反映 |
| Go heap size / GC pause | `runtime.ReadMemStats` | 内存压力 | 中（MemStats 有 STW）| 谨慎采样频率 |
| CPU utilization | `/proc` or runtime | 算力压力 | 低 | |
| Kafka consumer lag | Kafka API | 被上游多少条消息等着 | 中（需要 Kafka API 调用）| 可作为外部 reference |

**选项**：

- 选项 a (minimal)：pendingList + Publish p99
- 选项 b (recommended)：pendingList + per-user queue max + Publish p99 + advancer lag
- 选项 c (max)：再加 CPU / memory
- 选项 d (dynamic)：多指标加权打分，动态学习阈值

未定论。倾向 b 起步，生产观察后按需加。

### 3. 反压等级策略

| 方案 | 描述 | 优劣 |
|---|---|---|
| 2 级（on/off）| 要么全反压要么不反压 | 简单但粗糙，抖动大 |
| 3 级（normal / warn / halt）| 温和警告 + 硬熔断 | 实现简单，警告和熔断边界清晰 |
| 4 级（normal / yellow / red / critical）| 更细粒度 | 直观可运维，草案倾向 |
| 连续值（0-100）| gRPC 按比例 reject | 最平滑，但调试复杂 |

4 级草案作用矩阵：

| 等级 | 消费循环（B）| gRPC 入口（A）| 豁免 |
|---|---|---|---|
| normal | 正常 poll | 全通过 | — |
| yellow | 每批 poll 后 sleep 20ms | PlaceOrder 20% 返回 429 | CancelOrder / admin / health |
| red | 暂停 poll（每 2s 试一次）| PlaceOrder 100% + Transfer 50% 返回 429 | 同上 |
| critical | 持续暂停 poll | 所有非豁免 RPC 都 429 | 同上 |

### 4. 反压信号传递给 BFF / asset-service

| 方案 | 描述 | 优劣 |
|---|---|---|
| a. 只返回 429 | BFF 看到 429 本地 backoff | 简单, BFF 无法主动降速 |
| b. 响应 header 带 `X-Counter-Pressure: yellow/red/critical` | BFF 主动降速 | 实现简单, header 透传 gRPC metadata |
| c. Counter pressure 状态 publish 到 etcd | BFF watch etcd 主动限流 | 复杂, 跨服务依赖, 但可全局协调 |

草案倾向 **b**。但 BFF / asset-service 侧的集成需要对应 ADR 配套（可能是 ADR-0037 的扩展）。

### 5. 豁免列表

必须豁免的请求类型（即使反压 active 也要放行）：

| 请求 | 原因 |
|---|---|
| CancelOrder | 允许用户主动降压 |
| admin 接口 | 运维操作必须能执行 |
| healthcheck | 保持 LB 流量路由正常 |
| ADR-0062 evictor 内部调用 | 控制内存，比反压优先级高 |
| ADR-0060 advancer 的 PublishCheckpoint | 推进 watermark 比业务 event 优先级高 |

### 6. Hysteresis 策略

防止抖动：升级和降级阈值不对称。

例（pendingList 深度）：

```
yellow 升级: pending > 50000
yellow 降级: pending < 35000
red 升级: pending > 200000
red 降级: pending < 140000
critical 升级: pending > 500000
critical 降级: pending < 350000
```

30% 滞后是业界经验，具体数字需生产调优。

### 7. Detector 实现位置

| 方案 | 描述 | 优劣 |
|---|---|---|
| a. 每 vshard 独立 detector goroutine | per-vshard 反压状态独立 | 压力来自 vshard-level，天然匹配 |
| b. Counter node-level 单 detector | 全节点共享 pressure 状态 | 节点级资源压力反映更直接（CPU/mem），但粒度粗 |
| c. 混合（a 管业务指标 + b 管资源指标）| 分层反压 | 复杂 |

草案倾向 **a**，配合 ADR-0058 的 vshard 独立性；资源指标（CPU / mem）作为节点级 override（b 的最小子集）。

## 立项触发条件 (Trigger Conditions)

本 ADR 何时转 Proposed 立即实施：

### 硬触发（必须启动立项评估）

- Counter 生产环境出现一次 OOM / 进程崩溃由 pendingList 或 per-user queue 膨胀导致
- 单 vshard 的 pendingList 稳态深度 ≥ 10000 持续超 5 分钟
- TxnProducer.Publish p99 ≥ 1s（说明 Kafka 已有系统性压力）
- BFF / asset 侧出现大量 Counter gRPC 超时（≥ 1% 请求）

### 软触发（建议启动立项评估）

- 压测显示 Counter 在峰值 TPS 下会触达 ADR-0060 的 5s Publish panic
- 做市商高频场景下单 user queue 深度峰值 ≥ 10000
- 业务预估 6 个月内 TPS 将翻倍增长

## 候选实施阶段（立项后才执行）

仅作参考，立项时重新评估：

- **Phase 1**：Detector goroutine + 指标采集（pendingList + Publish p99 起步）
- **Phase 2**：消费循环反压（面 B，降 poll / 暂停 poll）
- **Phase 3**：gRPC 入口反压（面 A，返回 429 + `X-Counter-Pressure` header）
- **Phase 4**：Hysteresis + 豁免列表
- **Phase 5**：BFF / asset 侧消费 `X-Counter-Pressure` header 的 backoff 策略（需要对应服务的 ADR 配套）
- **Phase 6**：生产调优（阈值、滞后系数）
- **Phase 7**：极端场景演练（kill Kafka broker / 模拟做市商 DDoS 自家系统）

## 备选方案 (Alternatives Considered)

### 备选 1：永不反压，只靠 ADR-0060 的 panic 熔断

**拒绝**：熔断粒度粗（整 vshard 冷切迁移），用户体验差；反压是渐进式 degradation，优于直接 crash。

### 备选 2：反压全部放在 BFF 侧（基于 Counter 的 metrics 决策）

**拒绝**：
- BFF 无法看到 Counter 内部 pendingList / per-user queue 深度（指标不暴露 gRPC 太细粒度）
- 跨服务反馈链路长（BFF 看 Counter metrics → 决策 → 拒绝新请求），响应延迟高
- ADR-0037 的 BFF 速率限制是 **per-user rate**，不是 **Counter 内部压力反馈**，两者互补但不替代

### 备选 3：完全用 Kafka consumer lag 做反压信号

**拒绝**：Kafka lag 只反映消费面积压，反映不了 per-user queue 热点或 Publish 慢；且 Kafka API 查 lag 需要额外请求，成本不低。作为辅助信号 OK，作为唯一信号不足。

### 备选 4：引入 Istio / service mesh 的 circuit breaker

**拒绝**：service mesh 的 circuit breaker 是面向网络层（HTTP 5xx / 超时）的，不感知 Counter 业务语义；且 OpenTrade 当前没引入 service mesh，单为反压引入过重。

## 理由 (Rationale)

### 为什么单独起 ADR 而不并入 0060

- 反压是运行时 behavior control，和 ADR-0060 的消费模型结构性改造不同一个 scope
- 反压阈值需要生产基准，无法和 ADR-0060 一起设计
- BFF / asset-service 侧的反压响应策略涉及其他服务的 ADR，独立 ADR 便于引用

### 为什么先做调研不做实施

- 关键参数（阈值、滞后系数、等级划分）无法凭空设计
- 现阶段 Counter 还没上线，观察不到真实压力画像
- 立项前先把设计空间、决策点、候选方案梳理清楚，真要做时直接拿这份 ADR 跳过"从零讨论"阶段

### 为什么豁免列表必须包含 evictor 和 advancer

这两个是 Counter 内部的**自洽机制**：
- evictor 控制 byID 增长（ADR-0062），反压下仍必须运行否则内存雪崩
- advancer 推进 watermark（ADR-0060），反压下仍必须运行否则 snapshot pipeline 滞后

两者都不是"外部请求"，不消耗"外部配额"，没理由被反压拦截。

## 影响 (Consequences)

### 正面

- 反压设计空间系统化记录，未来立项直接用
- 触发条件量化，避免"什么时候该做反压"的模糊讨论
- ADR-0060 / 0062 的实现可以提前考虑指标暴露，减少未来改动

### 负面 / 代价

- 本 ADR 不产生代码，短期无直接价值
- 反压阈值随系统演进会过时，需要定期 review

### 中性

- 维持当前"无反压 + 熔断"状态，至 ADR 触发条件满足

## 已知不防御场景 (Non-Goals / Known Gaps)

### G1: 反压本身可能被误触发（false positive）

若阈值配低，正常业务也会触发反压，导致 gRPC 拒绝 → 用户体验降级。

**缓解**：生产上线后的调优阶段专门处理；hysteresis 降低抖动；灰度发布。

### G2: 反压信号传导到 BFF 但 BFF 不配合

如果 `X-Counter-Pressure` header 方案落地但 BFF 侧没做对应 backoff，header 等于白设。

**缓解**：BFF 侧反压响应作为 Phase 5 的强依赖，ADR-0063 立项时必须配套 BFF 侧 ADR。

### G3: 反压不能解决系统性故障

Kafka 集群挂、etcd 挂、存储故障，反压救不了（这些是 ADR-0060 G1/G2/G3 的范围）。反压只处理 **渐进式过载**，不处理 **catastrophic failure**。

**区分**：反压 ≠ 熔断。两者互补，不替代。

### G4: 热点单 user 的反压粒度

当前草案里反压是 vshard-level，一个热点 user 会把整个 vshard 的 yellow 触发，影响其他 user。

**缓解方向**（立项时考虑）：
- per-user rate limit（和 ADR-0037 协作）
- ADR-0058 热点 vshard 迁移（vshard 级别的负载隔离）

### G5: 反压和 ADR-0060 5s Publish panic 的协同

如果反压生效时 Publish p99 仍然恶化到 5s，还是会 panic。反压延缓但不消除熔断。

**预期行为**：反压起到"延缓"作用（给 Kafka / Counter 恢复时间），如果上游真的打爆了 panic 兜底。两个机制应该按"温和 → 激进"顺序生效。

## 实施约束 (Implementation Notes)

立项后实施时注意：

- Detector goroutine 采集指标不能持 vshard 关键锁
- `X-Counter-Pressure` header 在 gRPC trailer 还是 header 需要决定（trailer 对 streaming RPC 友好，header 对 unary RPC 更及时）
- BFF 侧的 backoff 策略（exponential / linear / token bucket）需要对应 ADR
- 豁免列表的实现：gRPC interceptor 按 method name 查表最简单
- 反压状态暴露到 Prometheus：`counter_pressure_level{vshard} = 0/1/2/3`
- Runbook：反压激活告警 → 人工决策是否 scale out / migrate vshard / 限流上游

## 参考 (References)

### 项目内

- ADR-0003: Counter ↔ Match 异步解耦
- ADR-0037: BFF 速率限制
- ADR-0057: Asset service 的同步调用契约
- ADR-0058: Counter vshard 隔离
- ADR-0060: Counter 消费异步化（提供指标采集基础）
- ADR-0062: 订单终态 evict（评估豁免）

### 讨论

- 2026-04-22: 用户与 Claude 关于 "Counter 处理不过来要不要反压" 的简短讨论（会话记录）

## 后续工作建议

- [ ] ADR-0060 上线后收集基准负载画像，作为阈值设计输入
- [ ] 和 BFF / asset-service owner 对齐反压响应策略（Phase 5 前置）
- [ ] 评估是否把部分反压能力借助 Istio / Envoy 实现（备选 4 的再评估）
- [ ] 每 6 个月回顾本 ADR，检查触发条件是否接近
- [ ] 立项时本 ADR 转 Proposed，补齐里程碑和阈值
