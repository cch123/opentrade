# OpenTrade

OpenTrade 是一个面向现货交易的中心化加密货币交易所（CEX）核心系统。仓库采用 Go monorepo，覆盖下单网关、账户清算、撮合、行情、WebSocket 推送、触发单、资金钱包、历史查询、管理平面、持久化投影与 snapshot 生产。

> **状态**：MVP 功能已基本闭环，仍处于工程验证与性能打磨阶段，不按生产可用系统发布。

## 架构速览

OpenTrade 把交易链路拆成几个单一职责服务：BFF 接入客户端，Counter 维护账户与订单状态，Match 维护 per-symbol orderbook，Kafka 承接服务间事件流，trade-dump 生成 MySQL 读模型与恢复 snapshot，Push/Quote/History/Trigger/Asset/Admin Gateway 分别处理实时推送、行情、只读查询、条件单、资金钱包和运维入口。

```
Client REST/WS
      |
      v
     BFF  ---- gRPC ----> Counter ---- order-event ----> Match
      |                    |  ^                            |
      |                    |  |                            v
      |                    |  +------ trade-event <---------+
      |                    |
      |                    +------ counter-journal
      |                               |
      v                               v
    Push <---- market-data ---- Quote / trade-dump ----> MySQL ----> History
                                      |
                                      +---- snapshot blobstore
```

核心设计原则：

- **Counter 是账户真值**：余额、冻结、订单生命周期和资金预留只在 Counter 内做权威变更。
- **Match 是 orderbook 真值**：撮合按 symbol 单线程 actor 执行，账户结算回流 Counter。
- **Kafka 是跨服务事件边界**：服务间用 journal / order-event / trade-event / market-data 等 topic 串联。
- **读写分离**：热路径查 Counter，历史列表走 trade-dump MySQL projection + History gRPC。
- **恢复依赖 snapshot + offset 原子性**：关键服务 snapshot 绑定 Kafka offset，避免恢复时漏事件或重放错位。

更完整的目标、数据流与故障模型见 [docs/architecture.md](docs/architecture.md) 和 [docs/adr/](docs/adr/README.md)。

## 服务与模块

| 路径 | 职责 |
|---|---|
| [api/](api/) | Protobuf 契约与生成代码 |
| [pkg/](pkg/) | 公共库：Kafka、decimal、auth、metrics、etcd config、snapshot、shard 等 |
| [bff/](bff/) | REST + WebSocket 网关，鉴权、限流、market-data cache |
| [counter/](counter/) | 账户、订单状态机、资金冻结、Reservation、user vshard |
| [match/](match/) | 撮合引擎，per-symbol orderbook 与成交事件 |
| [quote/](quote/) | 行情投影，深度、逐笔、K 线与 market-data 发布 |
| [push/](push/) | WebSocket fanout，公共行情与私有用户事件推送 |
| [trigger/](trigger/) | 止损、止盈、OCO、trailing stop 等触发单 |
| [asset/](asset/) | 资金钱包、内部转账与 asset journal |
| [trade-dump/](trade-dump/) | Kafka 事件持久化、MySQL projection、counter/trigger snapshot pipeline |
| [history/](history/) | 只读历史查询服务，读取 trade-dump 投影 |
| [admin-gateway/](admin-gateway/) | 内部运维入口，symbol 管理、灰度切换、批量撤单与审计 |
| [tools/](tools/) | TUI、Web UI、precision 配置工具 |
| [deploy/](deploy/) | 本地依赖、Docker Compose、smoke 脚本 |
| [docs/](docs/) | 架构、ADR、runbook、API 索引、研发记录 |

## 快速开始

### 依赖

- Go 1.26+
- Docker + Docker Compose
- `make`
- `buf`（运行 `make proto` 时需要）
- `protoc` 与 Go protobuf 插件（修改 proto 时需要）

### 一键冒烟

```bash
go work sync
make dev-up
./deploy/scripts/smoke.sh
```

`smoke.sh` 会构建核心服务、拉起本地进程、给两个用户充值、撮合一组 BTC-USDT 订单、校验 MySQL 投影，并在退出时清理服务进程。需要保留服务继续手工调试时使用：

```bash
KEEP=1 ./deploy/scripts/smoke.sh
```

详细步骤与常见失败原因见 [docs/smoke.md](docs/smoke.md)。

## 常用命令

```bash
# 生成 protobuf 代码
make proto

# 编译所有服务 module
make build

# 运行所有单元测试
make test

# go vet 所有服务 module
make vet

# 启动 / 停止本地 Kafka、etcd、MySQL、MinIO
make dev-up
make dev-down
```

每个目录都是独立 Go module，根目录通过 [go.work](go.work) 联动。开发某个服务时可以直接进入对应目录：

```bash
cd counter
go test ./...
go build ./...
```

需要分步启动全栈、手工 curl、查看 Kafka / MySQL 时，参考 [docs/dev-setup.md](docs/dev-setup.md)。

## 文档入口

- [架构总览](docs/architecture.md) - 系统目标、服务职责、核心数据流、性能目标
- [MVP Roadmap](docs/roadmap.md) - 已完成能力、待办项和历史演进
- [API 契约索引](docs/api/README.md) - BFF REST、WebSocket、内部 gRPC 导航
- [本地开发环境](docs/dev-setup.md) - 本地依赖、分步启动、调试命令
- [端到端冒烟](docs/smoke.md) - `smoke.sh` 的使用方式和排障指南
- [术语表](docs/glossary.md) - Counter、Match、sequence id、订单状态等术语
- [非目标](docs/non-goals.md) - 明确不做的方向及原因
- [ADR 索引](docs/adr/README.md) - 架构决策记录
- [Counter Runbook](docs/runbook-counter.md) - Counter vshard、HA、迁移与运维流程

## 贡献约定

- 涉及架构边界、数据一致性、恢复模型或对外协议的变更，应补充或更新 ADR。
- 修改 proto 后运行 `make proto`，并提交生成代码。
- 提交前至少运行相关 module 的 `go test ./...`；跨服务改动优先运行 `make test` 和 `./deploy/scripts/smoke.sh`。
- 并发密集代码建议额外运行 `go test ./... -race`，尤其是 Counter、Match、Push、Trigger。
