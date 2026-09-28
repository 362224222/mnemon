# Memory Agent (experimental)

<p><strong>English</strong> · <a href="README.zh-CN.md">中文</a></p>

A research preview of Mnemon as a **memory agent**. It keeps conversations as raw, dated records and divides the work
of memory the way dual-process accounts divide thinking:

- **System 1**: a fast decision model, [Jev](https://typesafe.ai/blog/introducing-system-one-models-and-jev) from
  TypeSafe AI, makes many small yes/no judgments about the records a search returns, such as whether a record is
  needed or no longer current.
- **System 2**: an LLM writes a few search queries, names what the reply needs and composes the answer.
- **Consolidation**: a background pass consolidates each record once into an index linked to the records.

The agent runs as a second [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) (DSH) instance beside
the main agent and hands it a small View at every turn.

**Code, paper and run records:** [Grivn/mnemon-memory-agent](https://github.com/Grivn/mnemon-memory-agent)

## Results

- **LoCoMo 91.7%**, first of the 15 systems that answer with gpt-4.1-mini (Mnemon and the 14 systems OmniMemEval
  re-evaluated), from 3.8k tokens of context per question.
- **LongMemEval-S 94.4%** with DeepSeek-V4.1-Flash answering, on par with the best published results.
- On the same records, Jev separates gold evidence better than DeepSeek and gpt-4.1-mini and is 3–11× faster.

Details, baselines and caveats are in the paper, *Mnemon: Raw Records, Fast Judgments, Slow Thoughts*
([PDF](https://github.com/Grivn/mnemon-memory-agent/blob/master/docs/paper/main.pdf); arXiv link to follow).

## Status

- A frozen research snapshot. It is not part of the `mnemon` binary, its releases or Mnemon Agency, and the evaluated
  system does not use the `mnemon` binary.
- Its results will be brought step by step into Mnemon and [dsh-mnemon](https://github.com/omdsh-dev/dsh-mnemon).
- Next, DSH will serve as a micro agent kernel, with Mnemon running on it as a memory agent, the form this preview
  already takes.
