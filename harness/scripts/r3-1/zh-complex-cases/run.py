#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


REQUIRED_AGENTS = [
    "mnemon-planner",
    "mnemon-researcher",
    "mnemon-implementer",
    "mnemon-reviewer",
    "mnemon-integrator",
]


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Runner:
    def __init__(self, out_dir, profile, workspace_id):
        self.out_dir = Path(out_dir)
        self.profile = profile
        self.workspace_id = workspace_id
        self.commands = []
        self.seq = 0

    def run(self, label, args, stdin=""):
        self.seq += 1
        log = self.out_dir / f"{self.seq:02d}-{label}.log"
        proc = subprocess.run(args, input=stdin, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        log.write_text(proc.stdout + proc.stderr, encoding="utf-8")
        self.commands.append({
            "name": " ".join(redact_args(args)),
            "status": "ok" if proc.returncode == 0 else "failed",
            "exit_code": proc.returncode,
            "log": str(log),
        })
        return proc, log

    def multica(self, label, *args, stdin=""):
        base = ["multica", "--profile", self.profile]
        if self.workspace_id:
            base += ["--workspace-id", self.workspace_id]
        return self.run(label, base + list(args), stdin=stdin)


def redact_args(args):
    out = []
    skip_next = False
    for item in args:
        if skip_next:
            out.append("<redacted>")
            skip_next = False
            continue
        out.append(item)
        if item in {"--token", "--mnemon-control-token"}:
            skip_next = True
    return out


def load_json_text(text, fallback):
    try:
        return json.loads(text)
    except Exception:
        return fallback


def workspace_from_daemon(profile, out_dir, commands):
    proc = subprocess.run(["multica", "--profile", profile, "daemon", "status", "--output", "json"], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    log = Path(out_dir) / "00-daemon-status.log"
    log.write_text(proc.stdout + proc.stderr, encoding="utf-8")
    commands.append({"name": "multica --profile <profile> daemon status --output json", "status": "ok" if proc.returncode == 0 else "failed", "exit_code": proc.returncode, "log": str(log)})
    if proc.returncode != 0:
        return "", proc
    data = load_json_text(proc.stdout, {})
    workspaces = data.get("workspaces") or []
    if not workspaces:
        return "", proc
    return (workspaces[0] or {}).get("id", ""), proc


def issue_id(proc):
    data = load_json_text(proc.stdout, {})
    return data.get("id", "")


def first_run_id(text):
    data = load_json_text(text, [])
    if isinstance(data, dict):
        data = data.get("runs") or data.get("items") or []
    if not data:
        return ""
    return data[0].get("id") or data[0].get("task_id") or ""


def create_issue(runner, title, description, assignee="", parent=""):
    args = ["issue", "create", "--title", title, "--description", description, "--status", "todo", "--priority", "medium", "--output", "json"]
    if assignee:
        args += ["--assignee", assignee]
    if parent:
        args += ["--parent", parent]
    proc, _ = runner.multica("issue-create", *args)
    if proc.returncode != 0:
        return ""
    return issue_id(proc)


def add_comment(runner, issue, body):
    proc, _ = runner.multica("issue-comment", "issue", "comment", "add", issue, "--content-stdin", "--output", "json", stdin=body)
    return proc.returncode == 0


def poll_run(runner, issue, attempts=18, delay=5):
    last = ""
    for _ in range(attempts):
        proc, _ = runner.multica("issue-runs", "issue", "runs", issue, "--output", "json")
        last = proc.stdout
        if proc.returncode == 0:
            rid = first_run_id(proc.stdout)
            if rid:
                return rid
        time.sleep(delay)
    return first_run_id(last)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--workspace-id", default="")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    commands = []
    workspace_id = args.workspace_id
    if not workspace_id:
        workspace_id, daemon_proc = workspace_from_daemon(args.profile, out_dir, commands)
        if daemon_proc.returncode != 0:
            return write_summary(args, commands, [], [], [], skipped="Multica daemon/profile is not ready")
    if not workspace_id:
        return write_summary(args, commands, [], [], [], skipped="Multica workspace id is unavailable")

    runner = Runner(out_dir, args.profile, workspace_id)
    runner.commands.extend(commands)
    agents_proc, _ = runner.multica("agent-list", "agent", "list", "--output", "json")
    if agents_proc.returncode != 0:
        return write_summary(args, runner.commands, [], [], [], failed="agent list failed")
    agents = {item.get("name"): item for item in load_json_text(agents_proc.stdout, [])}
    missing_agents = [name for name in REQUIRED_AGENTS if name not in agents]
    if missing_agents:
        return write_summary(args, runner.commands, [], [], [], failed="missing agents: " + ", ".join(missing_agents))

    suffix = args.run_id.replace("T", "").replace("Z", "")
    session_title = f"中文验收-R3-1并发PoC上下文复用 {suffix}"
    session_desc = """背景:
本次验收模拟真实 OA 协作：同一发布窗口同时影响库存、退款风控和会员补偿。

共享上下文:
- ctx:发布窗口=最近一次发布同时影响库存同步、退款规则、会员补偿。
- ctx:风险登记=重复补偿、库存超卖、误拒退款、客服承诺不一致。
- ctx:证据索引=日志片段、指标快照、手工核对表、回滚预案。

验收意图:
触发多个 PoC、共享上下文复用、二轮 follow-up 和 integrator 汇总。"""
    session = create_issue(runner, session_title, session_desc)
    if not session:
        return write_summary(args, runner.commands, [], [], [], failed="session root create failed")

    shared_context_comment = """Mnemon 更新: 共享上下文

## 状态

display-only

## 摘要

ctx:发布窗口、ctx:风险登记、ctx:证据索引、ctx:运行手册在三个 PoC 中复用。该评论不触发 provider 执行，只作为 OA 可见上下文。

## 事件引用

event:zh-parallel-poc-overlap/shared-context"""
    add_comment(runner, session, shared_context_comment)

    poc_specs = [
        ("PoC-A", "华东仓库存偏差排查", [("mnemon-researcher", "库存同步链路与差异样本核验"), ("mnemon-reviewer", "库存超卖风险与回滚条件复核")]),
        ("PoC-B", "退款风控误杀排查", [("mnemon-researcher", "风控规则命中样本与误杀比例核验"), ("mnemon-implementer", "退款白名单临时运行手册")]),
        ("PoC-C", "会员权益补偿与客服口径", [("mnemon-implementer", "补偿脚本与重复补偿保护"), ("mnemon-reviewer", "客服承诺边界与发布风险复核")]),
    ]
    created = [{"kind": "session", "id": session, "title": session_title}]
    run_targets = []
    for poc_id, title, children in poc_specs:
        root_title = f"中文验收-{poc_id}/{title} {suffix}"
        root_desc = f"""任务目标:
围绕 {title} 完成第一轮 PoC 判断。

共享上下文:
引用 ctx:发布窗口、ctx:风险登记、ctx:证据索引。

要求:
planner 需要拆解证据、识别与其他 PoC 的重叠依赖，并避免重复创建 shared context。"""
        root_id = create_issue(runner, root_title, root_desc, "mnemon-planner", session)
        if not root_id:
            return write_summary(args, runner.commands, created, run_targets, [], failed=f"{poc_id} root create failed")
        created.append({"kind": "poc-root", "poc_id": poc_id, "id": root_id, "title": root_title})
        run_targets.append(root_id)
        add_comment(runner, root_id, f"Mnemon 更新: {poc_id} 共享上下文引用\n\n## 摘要\n\n复用 ctx:发布窗口 与 ctx:风险登记，禁止复制成新的 canonical context。\n\n## 事件引用\n\nevent:{poc_id}/shared-context-ref")
        for agent, child_title in children:
            full_title = f"中文验收-{poc_id}/{child_title} {suffix}"
            desc = f"""任务目标:
{child_title}

共享上下文:
- ctx:发布窗口
- ctx:风险登记
- ctx:证据索引

本角色产出:
请给出中文结构化进展、证据、风险、下一步。不要直接判定最终结论。

回写提示:
accepted 后通过 Mnemon display writeback 写 Multica comment/metadata。"""
            child_id = create_issue(runner, full_title, desc, agent, root_id)
            if not child_id:
                return write_summary(args, runner.commands, created, run_targets, [], failed=f"{poc_id} child create failed")
            created.append({"kind": "assignment", "poc_id": poc_id, "id": child_id, "title": full_title, "agent": agent})
            run_targets.append(child_id)

    follow_title = f"中文验收-follow-up/跨PoC证据冲突对齐 {suffix}"
    follow_desc = """Round 2 follow-up:
请对齐 PoC-A 库存差异、PoC-B 退款误杀、PoC-C 会员补偿之间的共享发布窗口。

必须引用:
- ctx:发布窗口
- ctx:风险登记
- 三个 PoC 第一轮 issue

目标:
给出继续观察、局部补偿、灰度回滚和客服统一口径的条件。"""
    follow_id = create_issue(runner, follow_title, follow_desc, "mnemon-integrator", session)
    if not follow_id:
        return write_summary(args, runner.commands, created, run_targets, [], failed="follow-up create failed")
    created.append({"kind": "follow-up", "id": follow_id, "title": follow_title, "agent": "mnemon-integrator"})
    run_targets.append(follow_id)

    run_ids = []
    for issue in run_targets:
        rid = poll_run(runner, issue, attempts=6, delay=5)
        if rid:
            run_ids.append({"issue_id": issue, "run_id": rid})

    return write_summary(args, runner.commands, created, run_targets, run_ids)


def write_summary(args, commands, created, run_targets, run_ids, skipped="", failed=""):
    logs = "\n".join(Path(cmd["log"]).read_text(encoding="utf-8", errors="replace") for cmd in commands if Path(cmd["log"]).exists())
    assertions = [
        {"name": "created session root plus three PoC roots", "passed": len([x for x in created if x.get("kind") == "session"]) == 1 and len([x for x in created if x.get("kind") == "poc-root"]) == 3},
        {"name": "created at least six role assignments", "passed": len([x for x in created if x.get("kind") == "assignment"]) >= 6},
        {"name": "created follow-up issue for second round", "passed": any(x.get("kind") == "follow-up" for x in created)},
        {"name": "at least three assigned issues produced runs", "passed": len(run_ids) >= 3},
        {"name": "multiple PoCs share context comments", "passed": "ctx:发布窗口" in logs and "ctx:风险登记" in logs},
        {"name": "logs do not expose Multica token literal", "passed": "mul_" not in logs},
    ]
    failures = []
    skipped_items = []
    if skipped:
        skipped_items.append({"category": "skipped_missing_capability", "detail": skipped})
    if failed:
        failures.append({"category": "live_failure", "name": "zh complex Multica case", "detail": failed})
    if not skipped and not failed and not all(item["passed"] for item in assertions):
        failures.append({"category": "acceptance_failure", "name": "zh complex Multica case assertions", "detail": "one or more Chinese complex case assertions failed"})
    status = "skipped" if skipped_items else ("ok" if not failures else "failed")
    summary = {
        "schema_version": 1,
        "phase": "zh-complex-cases",
        "status": status,
        "started_at": os.environ.get("MNEMON_R3_PHASE_STARTED_AT") or now(),
        "finished_at": now(),
        "commands": commands,
        "assertions": assertions,
        "skipped": skipped_items,
        "failures": failures,
        "created": created,
        "run_targets": run_targets,
        "runs": run_ids,
    }
    Path(args.summary).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary).write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if status in {"ok", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
