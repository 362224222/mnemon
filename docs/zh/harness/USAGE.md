# Mnemon Harness Usage

以下命令假设已经构建：

```sh
go build -o mnemon .
go -C harness build -o ../mnemon-harness ./cmd/mnemon-harness
```

## 1. 安装 Agent Integration

把 Agent Integration 安装到当前项目：

```sh
./mnemon-harness setup --host codex --project-root .
```

使用 `--dry-run` 预览文件变化：

```sh
./mnemon-harness setup --host codex --project-root . --dry-run
```

## 2. 运行 Local Mnemon

启动 host integration 使用的本地服务：

```sh
./mnemon-harness local run
```

查看本地状态：

```sh
./mnemon-harness local status
./mnemon-harness status
```

## 3. Remote Workspace Sync

连接 Remote Workspace：

```sh
./mnemon-harness sync connect my-workspace
```

执行一次 push 或 pull：

```sh
./mnemon-harness sync push --once
./mnemon-harness sync pull --once
```

运行后台同步：

```sh
./mnemon-harness sync run --background
```

## 4. 验证声明

仓库维护者可以运行确定性测试与真实集成测试：

```sh
make test
make test-integration
```

普通 CI 只运行 `make test`；涉及 CLI E2E、时序、进程、传输或 Docker
边界时，才显式运行 `make test-integration`。付费的 Pi/DeepSeek 场景使用
`make test-live`。这些是开发检查，不是普通用户工作流的一部分。

## Trust model — a governance contract, not a sandbox

本地边界由协议和工程闸门执行（identity stamping、scope clamping、fail-closed
config、durable audit），**不是** OS 级隔离：同一用户下的恶意进程仍然可以读取本地文件。
各层实际承诺如下：

- **T0（始终）：** governance contract；wire 只接收 observations，kernel 是唯一 writer，
  每个 decision 都可归因。
- **T1（当前）：** 本地加固；私有 state tree（`.mnemon/harness`、其 `local`/
  `channel` 目录以及两个 credentials 目录）保持 owner-only（0700，setup rerun 会修正）；
  token 为 0600；`local run` 默认拒绝非 loopback listen address，除非显式传入
  `--allow-nonloopback`；`mnemon-harness token rotate --principal <p>` 会强制轮转 bearer
  token（撤销即轮转；token 启动时加载，因此需要重启 `local run` 生效）。
- **T2（remote phase）：** authn/authz、transport encryption 和 audit 是 remote
  coordination plane 的 admission 条件，而不是事后补丁。
- **T3（ecosystem phase）：** signature chains 和 sandboxed rules。

OS/process 级隔离明确**不属于** T0/T1 承诺。
