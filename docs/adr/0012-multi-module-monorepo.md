# ADR-0012: 采用 multi-module monorepo + Go workspace

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude

## 背景

项目有 6+ 个服务（BFF、counter、match、push、quote、trade-dump）+ 共享库（api、pkg）。代码组织方式选择：

1. 单 module monorepo（一个 `go.mod` 管所有）
2. 每个服务独立 repo（多仓库）
3. Multi-module monorepo（单仓库，每服务独立 `go.mod`）

## 决策

**采用 multi-module monorepo + Go workspace (`go.work`)。**
- 每个服务一个独立 module，拥有自己的 `go.mod`
- `api/`、`pkg/` 作为独立基础 module
- 根目录 `go.work` 统一管理所有 module，开发期本地 replace

## 备选方案

### 方案 A：单 module monorepo
- 优点：最简单，无 module 边界
- 缺点：
  - 任何包改动都影响全仓 build cache
  - 依赖管理粒度粗（所有服务共享完全相同的依赖版本）
  - 不方便独立发布

### 方案 B：多 repo
- 优点：服务完全独立
- 缺点：
  - 共享库变更需要多步（改 pkg → tag → 升级每个服务）
  - 跨服务重构困难
  - CI/CD 配置重复
  - 代码审查链路长

### 方案 C（选中）：Multi-module monorepo
- 优点：
  - 独立 `go.mod` = 独立依赖管理、独立发版能力
  - `go.work` 本地开发无感（保留单仓可见性）
  - 一次 PR 可跨服务改动
  - 每服务 CI build 只 rebuild 受影响部分
- 缺点：
  - Go workspace 是 1.18+ 特性，团队需要熟悉
  - 发布时需要 module 版本协调（通过 tag 或 sumdb）

## 理由

- 服务数量适中（6+），单 module 会混乱
- 共享库（api/pkg）频繁变更，多 repo 发版流程成本高
- Go 1.18+ workspace 已稳定，开发体验好
- 未来如果某服务真的要独立 repo，从 monorepo 拆出来容易

## 影响

### 正面
- 开发期无 module 版本困扰
- 跨服务重构方便
- 每服务可选择独立依赖版本（例如旧服务卡在旧 kafka 客户端版本）

### 负面
- 初始化 6+ 个 go.mod 文件，样板代码多一些
- CI 需要能识别变更范围并增量 build

### 中性
- 发布策略待定：全仓单一版本号，还是每服务独立版本号

## 实施约束

### 目录结构

见 `docs/architecture.md` §16.1。

### go.work 示例

```
go 1.22

use (
    ./api
    ./pkg
    ./counter
    ./match
    ./bff
    ./push
    ./quote
    ./trade-dump
)
```

### 依赖规则

- `api`、`pkg` 不依赖任何内部 module（纯基础）
- 各服务 module 依赖 `api` + `pkg`，**不互相依赖**
- 如发现服务 A 的代码被服务 B 用，提取到 `pkg/`

### go.mod 初始路径

- 采用伪路径：`github.com/xargin/opentrade/{module}`
  - 即便不 push GitHub，这是 Go 惯例路径
  - 将来如果真放到 GitHub/GitLab，路径无需改

### CI 策略（未来）

- PR 检测变更的 module，只 build + test 受影响服务
- `api` / `pkg` 变动触发所有服务 build

### 发布策略（未来）

- MVP：全仓单一 tag（`v0.1.0`），所有服务同步发版
- 稳定后：每服务独立 tag（`counter/v1.2.0`），分别发版

## 参考

- [Go workspace 官方文档](https://go.dev/ref/mod#workspaces)
- 讨论：2026-04-18 "monorepo vs multi-repo"
