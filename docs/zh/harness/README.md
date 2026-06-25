# Mnemon Harness Public Beta

`mnemon-harness` 是实验性 beta，用于安装 host-agent integration 资产，并把
它们连接到本地 Mnemon 服务。

稳定版 Mnemon 仍然是 memory CLI。Harness 只支持源码构建，没有兼容性保证，
当前范围限定在 Agent Integration、Local Mnemon、标准 event package 和 Remote Workspace sync。

## 1. 产品界面

面向用户的命令面刻意保持很小：

- `setup`: 安装 Agent Integration shim 资产。
- `local`: 运行或查看 Local Mnemon。
- `status`: 查看 Agent Integration、Local Mnemon 和 Remote Workspace 状态。
- `sync`: 把 Local Mnemon 连接到 Remote Workspace。

其他实现命令都是内部命令，不属于 beta 产品契约。

## 2. 当前范围

这个 beta 支持 Codex 和 Claude Code 投影。`.codex/`
和 `.claude/` 等 host 目录是生成出来的 surface。本地状态位于
`.mnemon/harness/`。

当前 beta 不承诺生产可用、自动 apply、多 agent governance、广义组织范围，
或通用 eval runtime。

## 3. 与稳定版 Mnemon 分离

`mnemon-harness` 从 `./harness/cmd/mnemon-harness` 构建。

除非用户显式开启 harness event emission 或直接运行 `mnemon-harness`，稳定版
`mnemon` 行为不变。

## 4. 试用

构建两个 binary：

```sh
go build -o mnemon .
go build -o mnemon-harness ./harness/cmd/mnemon-harness
```

为项目安装 Agent Integration：

```sh
./mnemon-harness setup --host codex --project-root .
./mnemon-harness local run
./mnemon-harness status
```

更多命令示例见 [USAGE.md](USAGE.md)。

## 5. 发布边界

这个 beta 只发布最小公开文档。内部计划、实验命令面、生成站点 HTML 和未来
governance 实验都不属于产品契约。
