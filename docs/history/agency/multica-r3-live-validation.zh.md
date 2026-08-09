# Multica R3 Live Validation

本文记录 R3 Multica 接入的维护者验证方法。它不是公开产品契约，
而是用于校准实现边界、复现实机验收和解释脚本行为的工程文档。

## 1. 源码校准结论

这次实现必须贴近 Multica 原生 runtime 模型，而不是把 Multica
重新解释成 MnemonHub、mailbox 或 projector。

关键源码约束来自 Multica 原仓库：

- `server/pkg/agent/codex.go`
  - Codex backend 启动的是 `codex app-server --listen stdio://`。
  - daemon 通过 JSON-RPC 保留 provider 原生的 thread、turn、tool message
    和 final answer 流。
  - 这说明 Mnemon wrapper 不应重新实现 Codex CLI 的每个输入输出结构；
    正确方向是透明保留 provider 流，再在流外接入 mnemond 能力。
- `server/internal/daemon/execenv/runtime_config.go`
  - Multica 通过 provider 原生发现机制注入运行时说明：Codex/Kimi 等写
    `AGENTS.md`，Claude 写 `CLAUDE.md`。
  - 注入内容把 issue/comment/status/mention 等 OA 操作暴露为 CLI 工作流。
  - 因此 Mnemon 也应该通过 skill/hook/command 让 agent 显式发出
    Mnemon event 或 surface report，而不是解析自由文本输出。
- `server/internal/service/task.go`
  - issue assignment 和 `@agent` mention 都会进入 `agent_task_queue`。
  - queue task 指向 agent runtime，daemon 再按 runtime claim task。
  - 这说明 Multica-hosted provider turn 与 mnemond-managed wake turn 都可以
    通过 Multica task 进入，但必须在 Mnemon 层保留不同 source 语义。
- `server/internal/handler/daemon.go`
  - claim task 会携带 agent identity、custom env、custom args、workspace、
    prior session/workdir、trigger comment 等上下文。
  - run messages 由 daemon 持久化并推送给 UI。
  - issue status/comment 是 OA 表达层；daemon 不自动替 agent 完成所有业务状态。

由此得到的 R3 原则：

- Mnemon 真相仍是 mnemond 接纳后的 EventEnvelope 和 governed resource。
- Multica 是 activation surface、provider hosting surface 和 OA 表达 surface。
- `mnemon-multica-runtime` 是 Multica-hosted surface adapter/provider wrapper，
  不是 hub backend、scheduler、mailbox 或 projector。
- 写回 Multica 的 comment/status/metadata 必须来自明确命令或 accepted event，
  不能通过解析 provider transcript 推断。
- display-only 写回不能触发 provider 执行；只有 assignment、mention、
  activation carrier 或 mnemond-managed wake 这类 activation lane 可以触发。

## 2. R3 数据流

```
+-------------------------+
| MnemonHub / local input |
| accepted EventEnvelope  |
+------------+------------+
             |
             v
+-------------------------+
| mnemond                 |
| policy + admission      |
| canonical resources     |
+------------+------------+
             |
             | accepted activation intent
             v
+-------------------------------+
| mnemon-multica-runtime        |
| surface adapter/provider wrap |
| - import Multica issue input  |
| - submit observed event       |
| - preserve provider stream    |
| - run explicit writeback cmd  |
+------+----------------+-------+
       |                |
       | activation     | display-only writeback
       v                v
+-------------+   +----------------------------+
| Multica task|   | Multica OA surface          |
| assignment  |   | issue status/comment/meta   |
| @agent      |   | run messages, child issues  |
| wake carrier|   | no provider trigger by self |
+------+------+   +-------------+--------------+
       |                        ^
       v                        |
+-------------+                 |
| Provider    |                 |
| Codex/Kimi  |                 |
| native flow |-----------------+
+-------------+ explicit commands:
                mnemon observe / surface-report
```

### Activation lane

Activation lane 决定是否拉起 provider：

- 人在 Multica 中 assign issue 或 `@agent`。
- mnemond-managed source 产生 `[mnemon:wake]`，再通过 Multica task 承载。
- accepted event 被转换成 activation carrier，carrier issue/comment 再触发
  Multica 原生 task。

Activation lane 可以使用 Multica 原生 `@agent` 语义，但这个事实不改变
Mnemon source 语义：`[mnemon:wake]` 仍属于 mnemond-managed source，
不是 adapter 自己定义的协议。

### Display lane

Display lane 只补全 OA 表达：

- 更新 issue status。
- 写入 final/comment feedback。
- 写入 Mnemon surface metadata，如 `mnemon.surface_role=display`、
  `mnemon.event_ref`、`mnemon.resource_ref`。
- 记录 artifact/evidence refs。

Display lane 不创建 provider task，不追加 `@agent` mention，不改变 Mnemon
canonical state。它只是把已经接纳的状态投影到 Multica UI。

## 3. Live 验收脚本

三个脚本用于复现实机验证：

- `scripts/multica_r3_live_prepare.sh`
  - 构建 `mnemon-acceptance`、`mnemon-harness`、`mnemon-multica-runtime`、
    `mnemond`。
  - 检查 Multica daemon/runtime 在线。
  - 根据 `.mnemon/harness/multica/registry.json` 同步 5 个 agent。
  - 为 agent 设置 provider wrapper 命令和 mnemond token/env。
- `scripts/multica_r3_live_provider.sh`
  - 作为验收用 deterministic provider wrapper。
  - 使用真实 Multica task/run/comment/status/child issue 链路。
  - 通过 `mnemon-harness multica activation-carrier` 触发 child work。
  - 通过 `mnemon-harness multica surface-report` 写回 display-only OA 结果。
- `scripts/multica_r3_live_acceptance.sh`
  - 顺序运行中文 case。
  - 默认开启 `--require-surface-flow`，要求 root/child 都有 run messages、
    comments、status 和 active agent 证据。

准备环境：

```sh
MNEMON_MULTICA_WORKSPACE_ID=<workspace-id> \
scripts/multica_r3_live_prepare.sh
```

运行默认 live case：

```sh
MNEMON_MULTICA_WORKSPACE_ID=<workspace-id> \
scripts/multica_r3_live_acceptance.sh
```

单独运行 overlap case：

```sh
MNEMON_MULTICA_WORKSPACE_ID=<workspace-id> \
scripts/multica_r3_live_acceptance.sh parallel-poc-overlap
```

## 4. 中文复杂 case

### r3-surface-readiness

目标是验证最小 R3 surface 链路：

- root issue 进入 planner。
- planner 创建 3 个 child carrier：
  - `surface-metadata-check`
  - `provider-run-visibility-check`
  - `activation-carrier-follow-up`
- child agents 分别写回 comment/status/metadata。
- 验收要求至少 3 个 active agents，root 和 child 都必须有 run messages。

### protocol-react-drill

目标是验证多轮 ReAct 风格协作：

- Observe：检查 root surface metadata 与 provider routing。
- Act：检查 OA writeback 不触发 provider。
- Reflect：integrator 整合残余风险。
- 验收要求 4 个 child issues、4 个 child terminal runs、全 5 个角色活跃。

### parallel-poc-overlap

目标是模拟多个 PoC 同时推进并复用上下文：

- `poc-runtime-routing`
- `poc-operator-runbook`
- `poc-release-risk`
- `follow-up-context-reuse`

前三个 PoC 共享 `ctx:evidence-ledger`、`ctx:provider-contract`、
`ctx:risk-register` 等上下文。follow-up issue 必须复用共享上下文并整合第一轮反馈。
验收要求 4 个 child issues、4 个 terminal child runs、全 5 个角色活跃。

## 5. 2026-07-01 验收记录

本轮在 Multica workspace `0925fd3e-ca35-4cb0-9bee-7d53070b7988`
和 profile `desktop-api.multica.ai` 上完成。

```
+-----------------------+----------+---------+------------+---------------+
| case                  | root     | child   | active     | report        |
+-----------------------+----------+---------+------------+---------------+
| r3-surface-readiness  | TEA-199  | 3       | 4 agents   | status=ok     |
| protocol-react-drill  | TEA-203  | 4       | 5 agents   | status=ok     |
| parallel-poc-overlap  | TEA-208  | 4       | 5 agents   | status=ok     |
+-----------------------+----------+---------+------------+---------------+
```

报告路径：

- `/tmp/mnemon-r3-multica-live/r3-surface-readiness-20260701T013810Z/acceptance-report.json`
- `/tmp/mnemon-r3-multica-live/protocol-react-drill-20260701T014050Z/acceptance-report.json`
- `/tmp/mnemon-r3-multica-live/parallel-poc-overlap-20260701T014209Z/acceptance-report.json`

关键证据：

- root runs 均包含 `text`、`tool_use`、`tool_result` message。
- root comments 均可读，并且由 surface-report 写入。
- child issues 均达到 `done`。
- child runs 均有 terminal feedback comments。
- metadata 使用 R3 keys：`mnemon.event_ref`、`mnemon.resource_ref`、
  `mnemon.surface_ref`、`mnemon.surface_role=display`。
- 未使用 legacy hub/mailbox metadata。

## 6. 仍需保持的边界

live provider wrapper 是验收工具，不代表最终产品形态。最终产品实现应该继续向
Multica 原生 runtime 靠拢：

- 优先透明代理 provider 原生协议流。
- 不复制 Codex/Kimi/Claude CLI 的完整输入输出结构。
- 不把 Multica issue/comment 当 canonical Mnemon state。
- 不通过 display-only projection 触发执行。
- 不从自由文本 transcript 中反推 Mnemon event。

只要这些边界保持，Multica UI 可以充分承担 OA 体验：issue 看板、状态、child issue、
run messages、comments、mentions、inbox 和 agent runtime 配置都可以被充分使用；
Mnemon 只保留事件治理和协作语义的权威位置。
