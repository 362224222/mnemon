# Memory Agent（实验性）

<p><a href="README.md">English</a> · <strong>中文</strong></p>

这是 Mnemon 以**记忆 Agent** 形态运行的研究预览。它把对话保存为带日期的原始记录，并按"双系统"理论的方式分工：

- **System 1**：快速的决策模型 [Jev](https://typesafe.ai/blog/introducing-system-one-models-and-jev)（TypeSafe AI），对检索返回的记录做大量简单的是非判断，例如这条记录是否需要、是否已经不再成立。
- **System 2**：一个 LLM，负责写少量检索查询、说明回复需要什么，并组织最终回答。
- **后台整合**：把每条记录整合一次，建成链接回记录的索引。

它作为第二个 [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness)（DSH）实例运行在主 Agent 旁边，每一轮交给主 Agent 一个精简的 View。

**代码、论文与运行记录：**[Grivn/mnemon-memory-agent](https://github.com/Grivn/mnemon-memory-agent)

## 结果

- **LoCoMo 91.7%**：在用 gpt-4.1-mini 答题的 15 个系统（Mnemon 与 OmniMemEval 重测的 14 个系统）中排名第一，每题只用 3.8k tokens 上下文。
- **LongMemEval-S 94.4%**：由 DeepSeek-V4.1-Flash 答题，与已公开的最好成绩持平。
- 在同一批记录上，Jev 区分关键证据的效果优于 DeepSeek 和 gpt-4.1-mini，速度快 3–11 倍。

细节、对比基线与注意事项见论文《Mnemon: Raw Records, Fast Judgments, Slow Thoughts》（[PDF](https://github.com/Grivn/mnemon-memory-agent/blob/master/docs/paper/main.pdf)；arXiv 链接稍后补充）。

## 状态

- 这是冻结的研究快照，不属于 `mnemon` 程序、它的发布版本或 Mnemon Agency；评测的系统没有用到 `mnemon` 二进制。
- 研究成果将逐步输送到 Mnemon 和 [dsh-mnemon](https://github.com/omdsh-dev/dsh-mnemon)。
- 下一步，DSH 将作为微型 Agent 内核，Mnemon 作为记忆 Agent 运行在它之上；这个预览就是这种形态。
