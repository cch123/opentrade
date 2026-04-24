# ADR-0049: Snapshot 默认 protobuf，debug 可选 JSON

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0036（quote state snapshot）, 0048（snapshot-offset + output flush barrier）

## 背景 (Context)

四个持久化内存态服务 —— counter / match / trigger / quote —— 的 snapshot 文件都是 JSON。JSON 的好处是 `jq` 能人眼排查、schema 演进宽松；代价是体积 2-3× 于等价 protobuf、encode/decode 占用 CPU。

触发点：ADR-0048 backlog item 4（per-user transfer_id ring，256 条/user）已经落地。体积估算：

- 10k 活跃 user × 256 × 50 B（raw id） ≈ 128 MB
- JSON 放大到 ~400 MB/snapshot
- `snapshot-interval=60s` → 每分钟写一次几百 MB 文件、fsync、rename

counter 还有 account/balance 双层 version (ADR-0048 backlog #1)、match_seq map (#2)、offsets (#3)、reservations、orders —— 都是"有 10k × 小字段"的乘法放大项。JSON 很快会成为写放大瓶颈。

protobuf 在"数值 + 定长字段"上天然紧凑（varint / 定 bits），在这个 workload 下预计 30-40% 于 JSON 体积。更重要的是：**proto schema 演进规则明确**（field number + reserved），适合长期稳定的内部格式。

JSON 仍然有价值：debug / 事故排查时，能直接看文件内容比反解 proto 快一个数量级。我们不想彻底丢掉这个能力。

## 决策 (Decision)

### 1. 默认 protobuf、flag 可切 JSON

每个服务 main 加 `--snapshot-format=proto|json`，默认 `proto`。环境变量 `OPENTRADE_SNAPSHOT_FORMAT` 作为全局覆盖（CI / 压测统一切换用）。

Save 严格按 flag 指定格式写；Load 按"先 proto、后 JSON"两步试（提供迁移窗口）。

### 2. Proto schema 位置：`api/snapshot/<svc>.proto`

和 `api/event/` / `api/rpc/` 平级：

```
api/
  event/
  rpc/
  snapshot/
    counter.proto
    match.proto
    trigger.proto
    quote.proto
  gen/
    snapshot/
      counter.pb.go
      ...
```

Snapshot schema 不是对外服务接口，但仍是**稳定的序列化合约**，和 event / rpc 的稳定性等级一样 —— 改字段要加 field number 保留旧的。放在 api/ 下让 protoc 工具链统一。

### 3. 文件命名：扩展名区分格式

```
./data/counter/shard-0.pb       (proto)
./data/counter/shard-0.json     (json)
./data/match/BTC-USDT.pb
./data/match/BTC-USDT.json
```

`Save(basePath, snap, format)` 内部拼 `basePath + "." + ext` 写入；`Load(basePath)` 优先试 `.pb`，再试 `.json`。atomic write (`tmp + rename`) 的 tmp 后缀复用扩展名，避免两个格式的 tmp 混淆。

### 4. 迁移路径

1. 部署新版本，默认 `--snapshot-format=proto`
2. 重启 primary：Load 发现旧 `.json` 存在 → load → state 恢复 → 正常运行
3. 第一次 snapshot tick 产出新 `.pb` 文件
4. 老 `.json` 保留在磁盘（不删）—— 至少保留一个 snapshot 周期做回滚兜底
5. 确认 `.pb` 稳定后，运维清理老 `.json`；或者加自动清理（见 §未来工作）

**回滚**：如果 proto 格式出问题，operator 可以 `--snapshot-format=json` 重启，老 `.json` 还在能走老路径。

### 5. Decimal / 枚举继续用 string / uint8

proto 里 decimal 继续是 `string`（和 JSON 一致），避免"定点整数 sat"带来的跨语言精度 bug、与现有 `pkg/dec` 对接零改动。枚举（Side / OrderType / Status）继续 `uint8`（proto 里用 `uint32` 容器 + 业务层 cast），保持 on-wire 字节数和老 JSON 近似。

### 6. Capture / Restore 逻辑不动，只换 encoder

每个服务的 `Capture(state, ...) *Snapshot` / `Restore(state, snap) error` 继续操作 in-memory struct（旧的 `ShardSnapshot` / `SymbolSnapshot` 之类）。新增 `EncodeProto(snap) ([]byte, error)` / `DecodeProto([]byte) (*Snapshot, error)` 做格式层翻译。Save / Load 是一层薄 routing。

好处：测试不改；业务代码零感知；新老格式 round-trip 有单点对照。

## 备选方案 (Alternatives Considered)

### A. 只有 proto，彻底去掉 JSON

- 省代码（一套 codec）
- 代价：事故时 debug 工具链要先反编 proto → 不直观
- 结论：拒。调试能力值得一条 flag

### B. CBOR / MessagePack

- 紧凑 + schema-less，不需要 .proto 文件
- 代价：跨语言 decimal 行为不统一、schema 演进无强约束；未来重构容易破坏兼容性
- 结论：拒。stable backend 服务值得 proto 的显式 schema

### C. 只做 counter，其他三服务继续 JSON

- 最小增量；但三个服务同一个问题（体积、解析慢）
- 结论：一次性推完四服务，保持一致范式；分批 commit 即可

### D. 压缩（gzip / zstd）overlay 在 JSON 上

- 确实能把 JSON 压到 proto 同级体积
- 代价：debug 工具链每次要解压；CPU 开销高于 proto；schema 演进还是 JSON 式弱校验
- 结论：如果以后还要压体积，proto + zstd 叠加即可；但首选 proto

### E. 文件头 magic bytes 区分格式

- Load 打开一个文件，读前几字节判断 proto / json
- 代价：Save 要写带 header 的自定义格式 → 不能直接 `jq` 读 JSON 了
- 结论：拒。扩展名方案保留"JSON 文件天然可读"的属性

## 理由 (Rationale)

- **体积 + CPU**：proto 对这类"大量小记录 + 数值字段"是直接胜
- **演进安全**：field number 保留 + reserved 语义让我们未来改字段不怕破坏老文件
- **debug 回退**：flag 一键切 JSON，事故现场排查不被工具链绑架
- **四服务统一**：同一套 flag + 文件命名 + Load fallback，ops 心智一致

## 影响 (Consequences)

### 正面

- snapshot 体积 / 写时间 / IO 压力减半或更多
- schema 改动有 proto 工具链守护
- 事故时 `--snapshot-format=json` 可用

### 负面 / 代价

- 每服务多一份 `.proto` schema（和对应 Capture struct 的映射代码）；~一屏消息 × 4
- Load 路径两步试（proto → json）—— 很小的启动开销
- 老 `.json` 迁移期并存 → 磁盘短时多占 2×；运维手册要说明清理时机
- 新增 `api/snapshot/` 目录 + `api/gen/snapshot/` 生成代码；Makefile 的 proto 生成步骤自动覆盖

### 中性

- CI pipeline 不动（protoc 已经在跑了）
- 跨服务 snapshot 版本独立演进，互不影响

## 实施约束 (Implementation Notes)

### Flag 约定（每服务）

```
--snapshot-format=proto  # default
--snapshot-format=json   # debug fallback
# env override:
OPENTRADE_SNAPSHOT_FORMAT=json   # 全局覆盖，优先级高于 flag（方便一次性灰度）
```

### 文件路径

- 之前 path `./data/counter/shard-0.json` → 现在 snapshotPath 返回**无扩展名** `./data/counter/shard-0`
- `Save(basePath, snap, format)` 内部拼 `.pb` / `.json`
- `Load(basePath)` 按以下顺序探测：
  1. `basePath + ".pb"` —— 存在就走 proto
  2. `basePath + ".json"` —— 存在就走 JSON
  3. 都不存在 → `os.ErrNotExist`，caller 做 cold start

### 分服务推进

- 批 3b-1 (counter pilot)：ADR + api/snapshot/counter.proto + encoder/decoder + flag + 测试
- 批 3b-2/3/4 (match / trigger / quote)：同模式复制，不再写新 ADR（ADR-0049 统一管辖）

每服务独立 commit，失败可独立回滚。

### 测试

- proto round-trip：`Capture → Encode(proto) → Decode → Restore → 对比 state`
- JSON round-trip：flag=json 路径同样测试
- 跨格式 Load：落 JSON、Load 期望找到；落 proto、Load 期望找到；两者都在 → proto 优先
- Version mismatch 行为：proto decode 新字段丢到 `Unknown` (proto3 默认)，老客户端跳过不报错

### 失败场景

| 情况 | 结果 |
|---|---|
| proto 文件存在但损坏 | 报错退出（启动失败，operator 介入） |
| proto 存在 + json 也存在 | 走 proto；json 保留为历史备份 |
| 都不存在 | cold start |
| --snapshot-format=proto 但只有老 json | Load 降级到 json；下一个 tick 写 proto，之后 Load 都走 proto |
| --snapshot-format=json 在只有 pb 的系统启动 | Load 依然会读到 pb（load 不受 flag 影响），只是下次 Save 会写 json 到并存文件。operator 需要手动清 pb 才能完全走 json 回退路径 |

## 未来工作 (Future Work)

- **自动清理老格式文件**：snapshot 写成功后 N 个 tick 删对应的老格式文件（需要配置化）
- **zstd 叠加**：proto + zstd 再压 50%，尤其 transfer_id ring 这种重复 prefix 多的内容
- **schema 分支对接 backup node（ADR-0006）**：backup 打 snapshot 上 S3 时统一用 proto，避免两套格式在链路里混

## 参考 (References)

- [ADR-0036](./0036-quote-state-snapshot.md) — quote state snapshot 首次引入 JSON
- [ADR-0048](./0048-snapshot-offset-atomicity.md) — snapshot + offset atomicity（本 ADR 的前置）
- backlog item 4（transfer ring）—— 体积放大触发点
- 实现（第一批）：
  - [api/snapshot/counter.proto](../../api/snapshot/counter.proto)
  - [counter/snapshot/snapshot.go](../../counter/snapshot/snapshot.go)
  - [counter/cmd/counter/main.go](../../counter/cmd/counter/main.go)
