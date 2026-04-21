# ADR-0011: Counter 提供统一的 Transfer 接口

- 状态: Superseded by [ADR-0057](./0057-asset-service-and-transfer-saga.md)（M4 落地于 commit ab343ef — `Counter.Transfer` 对外 gRPC 物理删除；Counter 进程内 `svc.Transfer` 函数作为 AssetHolder 三方法的共享实现基底保留）
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0004, 0015, 0018

## 背景

Counter 除了下单冻结外，还需要支持：
- 充值入账（未来由 wallet-service 调用）
- 提现扣减
- 独立的冻结/解冻（风控、人工操作等）
- 内部转账（站内 user-to-user，未来）

这些操作都需要走同样的用户定序、幂等、journal 记账流程。

## 决策

**Counter 暴露统一的 gRPC `Transfer` 接口**，通过 `Type` 字段区分业务类型，内部使用同一套用户定序（ADR-0018）和 journal 写入机制。

MVP 阶段只实现 deposit / withdraw / freeze / unfreeze 四种类型。

## 备选方案

### 方案 A：每个业务类型一个 RPC
- `Deposit`, `Withdraw`, `FreezeBalance`, `UnfreezeBalance` 各自独立
- 优点：接口语义清晰
- 缺点：
  - Counter 内部处理路径重复
  - 新增业务类型要新增 RPC
  - 客户端 stub 膨胀

### 方案 B（选中）：统一 Transfer
- 优点：
  - 一个入口，内部路径统一
  - 新增类型只需加枚举值
  - 客户端只依赖一个 RPC
- 缺点：
  - 字段需要允许部分可选（不同 Type 字段含义稍有差异）

## 理由

- 所有资金变动本质都是 `(user, asset, amount, reason)` 的账本操作
- 统一接口便于做权限控制、审计、速率限制
- MVP 阶段外部调用方少（主要是 wallet-service 和内部运维工具），统一接口开发成本低

## 影响

### 正面
- Counter 内部只需一条通用处理链
- 所有资金操作共享去重、定序、事务机制
- 未来新增类型成本低

### 负面
- 接口参数允许部分字段可选（如 BizRefID 对 deposit 必填，对 freeze 可选），client 侧需要遵循约定
- 若未来类型差异过大（如批量转账），可能需要细化

## 实施约束

### proto 定义

```protobuf
service CounterService {
    rpc Transfer(TransferRequest) returns (TransferResponse);
}

enum TransferType {
    TRANSFER_TYPE_UNSPECIFIED = 0;
    DEPOSIT   = 1;  // available += amount
    WITHDRAW  = 2;  // available -= amount
    FREEZE    = 3;  // available -= amount, frozen += amount
    UNFREEZE  = 4;  // frozen -= amount, available += amount
    // 未来:
    // TRADE_SETTLEMENT = 5;  // 仅 Counter 内部自己产生
    // INTERNAL_TRANSFER_OUT = 10;
    // INTERNAL_TRANSFER_IN = 11;
}

message TransferRequest {
    string user_id = 1;          // 路由 key
    string transfer_id = 2;      // 幂等键,外部生成,24h 窗口内不能重复
    string asset = 3;            // USDT, BTC, ETH, ...
    string amount = 4;           // decimal string
    TransferType type = 5;
    string biz_ref_id = 6;       // 业务关联 ID (chain_tx_hash / withdraw_order_id / etc)
    string memo = 7;
}

message TransferResponse {
    string transfer_id = 1;
    enum Status {
        STATUS_UNSPECIFIED = 0;
        CONFIRMED = 1;
        REJECTED = 2;
        DUPLICATED = 3;           // 命中去重 → 返回上次的结果
    }
    Status status = 2;
    string reject_reason = 3;
    BalanceSnapshot balance_after = 4;
}
```

### 处理流程

```
1. gRPC handler 收到 Transfer 请求
2. 路由校验: user_id hash 是否属于本 shard (非本 shard 返回错误,让 BFF 重路由)
3. UserSequencer.Submit(user_id, func(seqID) { ... })
   3a. dedup 查询: 若 transfer_id 存在 → 返回 DUPLICATED + 缓存结果
   3b. 内存读 balance, 校验 (余额不足 → REJECTED)
   3c. Kafka 写 counter-journal (TransferEvent), 单次写,幂等 producer
   3d. 若 Kafka 写失败 → 返回错误,内存不改
   3e. 更新内存 balance
   3f. dedup 记录 transfer_id + 结果 (7 天 TTL)
   3g. 返回 CONFIRMED + balance
```

### Dedup 表

- Redis（推荐）+ 本地内存 LRU（性能优化）双层
- Key: `dedup:transfer:{user_id}:{transfer_id}`
- Value: `{status, balance_after, ts}`
- TTL: 7 天

### 权限控制（MVP 预留）

- Counter gRPC server 挂 interceptor，校验调用方身份（mTLS 或 internal token）
- 只允许授权服务调用（wallet-service、admin-tool）
- BFF 不直接调 Transfer（BFF 主要是下单/撤单）

### 与 PlaceOrder 的关系

- `PlaceOrder` 内部也涉及冻结，但有撮合链路和 Kafka 事务语义
- 不复用 Transfer 内部实现（代码不同路径）
- 但都经过同一个 UserSequencer 保证用户定序

## 参考

- ADR-0004: counter-journal
- ADR-0015: 幂等处理
- ADR-0018: Counter Sequencer
- 讨论：2026-04-18 "支持 transfer 接口"
