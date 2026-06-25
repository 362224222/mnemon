# Mnemon Harness Usage

以下命令假设已经构建：

```sh
go build -o mnemon .
go build -o mnemon-harness ./harness/cmd/mnemon-harness
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

仓库维护者可以验证 harness loop、host 和 binding manifest：

```sh
make harness-validate
```

这是开发检查，不是普通用户工作流的一部分。
