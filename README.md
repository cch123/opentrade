# OpenTrade

Crypto spot CEX 交易系统（现货）。Monorepo，包含 BFF / Counter（柜台/清算）/ Match（撮合）/ Trigger（条件单）/ Asset（资金钱包）/ Push（WS 推送）/ Quote（行情）/ History（历史查询）/ Admin Gateway / trade-dump（持久化 + snapshot 生产）十个服务 + 公共库 + 运维工具。

> **状态**：MVP 阶段。

## 文档

- 架构总览：[`docs/architecture.md`](docs/architecture.md)
- MVP 进度与填坑：[`docs/roadmap.md`](docs/roadmap.md)
- 端到端冒烟：[`docs/smoke.md`](docs/smoke.md)
- 决策记录：[`docs/adr/`](docs/adr/README.md)
- 外部架构参考：[`docs/research/binance-ledger.md`](docs/research/binance-ledger.md)

## 仓库布局

```
opentrade/
├── api/              proto 定义 + 生成代码（独立 go module）
├── pkg/              公共库：kafka / dec / logx / metrics / clustering / connectx / ...
├── counter/          柜台/清算（用户余额、订单、Reservation；ADR-0058 vshard 模型）
├── match/            撮合服务（per-symbol orderbook + 单线程撮合）
├── trigger/          条件单（stop-loss / take-profit / OCO / trailing；ADR-0040~46）
├── asset/            资金钱包（充提冻 / 内部转账；ADR-0065）
├── bff/              API 网关（REST + WS + 鉴权 + 限流 + market-data cache）
├── push/             WebSocket 推送（按 user_id sticky）
├── quote/            行情聚合（深度 / K 线 / 逐笔）
├── history/          只读历史查询（订单 / 成交 / 流水 / 触发单；ADR-0046）
├── admin-gateway/    运维网关（rollout / 灰度切换 / 内部工具）
├── trade-dump/       持久化 + snapshot 生产（MySQL projection + ShadowEngine）
├── tools/
│   ├── tui/          运维 TUI
│   ├── web/          运维 Web
│   └── precision-cli/ symbol precision 配置工具
├── deploy/
│   ├── docker/       docker-compose 本地依赖（kafka / etcd / mysql / minio）
│   └── scripts/      运维脚本（部署 / 切主 / 迁移）
└── docs/
    ├── architecture.md
    ├── runbook-counter.md
    └── adr/
```

## 开发环境准备

### 依赖

- Go 1.26+
- Protocol Buffers 编译器：`protoc`
- Docker + Docker Compose（用于本地起中间件）

### 一次性安装

```bash
# proto 生成器
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# 确认 $GOBIN 或 $(go env GOPATH)/bin 在 PATH 中
export PATH="$PATH:$(go env GOPATH)/bin"
```

### 常用命令

```bash
# 生成 protobuf 代码
make proto

# 编译所有服务
make build

# 运行所有单元测试
make test

# 启动本地依赖（kafka/etcd/mysql/minio）
make dev-up

# 停止本地依赖
make dev-down

# 清理生成产物
make clean
```

## 模块开发

每个服务是一个独立 Go module，根目录 `go.work` 统一联动：

```bash
cd counter
go build ./...
go test ./...
```

或在根目录：

```bash
go build ./...
go test ./...
```

## 提交规范

- 单个改动附带必要的 ADR（若涉及架构决策）
- 所有代码须 `go vet ./...` 通过
- 修改 proto 后需重新 `make proto` 并提交生成代码
