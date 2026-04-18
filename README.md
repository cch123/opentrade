# OpenTrade

Crypto spot CEX 交易系统（现货）。Monorepo，包含 BFF、Counter（柜台/清算）、Match（撮合）、Push、Quote、trade-dump 六个服务 + 公共库。

> **状态**：MVP 阶段。

## 文档

- 架构总览：[`docs/architecture.md`](docs/architecture.md)
- MVP 进度与填坑：[`docs/roadmap.md`](docs/roadmap.md)
- 端到端冒烟：[`docs/smoke.md`](docs/smoke.md)
- 决策记录：[`docs/adr/`](docs/adr/README.md)

## 仓库布局

```
opentrade/
├── api/              proto 定义 + 生成代码（独立 go module）
├── pkg/              公共库：kafka / decimal / logx / metrics / ...（独立 go module）
├── counter/          柜台服务
├── match/            撮合服务
├── bff/              API 网关（REST + WS）
├── push/             WebSocket 推送
├── quote/            行情服务
├── trade-dump/       MySQL 落库
├── deploy/
│   ├── docker/       docker-compose 本地依赖（kafka / etcd / mysql / minio）
│   └── scripts/      运维脚本（切主 / 迁移 symbol / ...）
└── docs/
    ├── architecture.md
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
