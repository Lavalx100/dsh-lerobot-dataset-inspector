---
description: "安装此 profile 层，以检查本地 LeRobot v2.0、v2.1 与 v3.0 元数据、清单和有界资产目录。"
kind: "package-bundle"
---

# DSH LeRobot 数据集检查器

[English](README.md) | 中文

## 摘要

将 `lerobot_inspect_dataset` 加入 profile，即可检查本地 LeRobot 数据集。它会汇总元数据、特征、统计数据、任务、episode 和资产数量，并可选返回一个 episode 的详情。插件通过当前 DSH 文件系统读取数据，不会打开机器人数据 Parquet 文件或解码视频帧。

## 目录

- [使用此包](#use-this-package)
- [了解实现](#understand-the-implementation)
- [延伸阅读](#further-exploration)
- [模型体验](#model-experience)
- [已知限制与延期工作](#known-limitations-and-deferred-work)
- [开发备注](#dev-note)

-----

<a id="use-this-package"></a>
## 使用此包

### 安装到 profile

在 DSH 检出目录或已安装的 DSH CLI 中，将此本地 bundle 加入 profile：

```sh
dsh plugin --profile <profile> add /absolute/path/to/lerobot-dataset-inspector
dsh plugin --profile <profile> remove dsh-lerobot-dataset-inspector
```

使用该 profile 启动 DSH，然后将文件系统可见的数据集目录传给 `lerobot_inspect_dataset`。移除此包也会移除对应的 profile 层。

### 可用功能

此工具会返回 LeRobot 版本与数量、特征形状、可用的聚合统计数据、任务名称、episode 长度摘要以及数据和视频文件数量。传入 `episode_index` 可包含该 episode 的元数据；对于 v3.0，也会包含其单 episode 统计数据。

此工具通过 `ctx.fs` 读取元数据及 episode 和任务清单。它不会打开数据 Parquet 文件，也不会读取或解码 MP4 内容。所有路径仍受当前 profile 的文件系统权限约束。

### 运行测试

测试使用小型元数据文件和 Parquet 固定样本。测试不会访问机器人数据集或网络服务。

```sh
pnpm install --ignore-workspace --ignore-scripts
node --test tests/inspector.test.mjs
node --experimental-test-coverage --test tests/inspector.test.mjs
```

此包需要 Node.js 24.9 或更新版本，以及 DSH `>=0.2.2-alpha.2 <0.3.0`。

-----

<a id="understand-the-implementation"></a>
## 了解实现

<details>
<summary>实现细节——点击展开</summary>

此 bundle patch 会插入一条 Cordis 插件行。插件通过 profile 文件系统解析数据集路径，只读取元数据及 episode 和任务清单，并对文件、行、字节和目录深度应用扫描上限。扫描达到限制时，结果会标明数据不完整及数量下限。

Patch 和源码索引分别见 [`cordis.patch.yml`](cordis.patch.yml)、[`src/index.mjs`](src/index.mjs) 和 [`src/inspect.mjs`](src/inspect.mjs)。Parquet 解析使用固定版本的 `hyparquet` 依赖。

</details>

-----

<a id="further-exploration"></a>
## 延伸阅读

有关分享独立插件和参与社区的方式，请参阅 [DeepSeek Harness 贡献指南](https://github.com/deepseek-ai/deepseek-harness/blob/master/CONTRIBUTING.md)。

-----

<a id="model-experience"></a>
## 模型体验

此工具返回结构化元数据和有界摘要。它不会向模型暴露机器人帧内容或解码后的视频；指定的 v3.0 episode 可以包含其存储的单 episode 统计数据。

#### KV Cache 影响

特征、任务、行数和输出都有上限；不过，较大的元数据摘要仍会比小型数据集占用更多上下文。

-----

## 已知限制与延期工作

此检查器不会校验机器人帧值或解码视频。文件、条目或深度限制阻止完整扫描时，结果会标明目录清单不完整。

- **元数据读取有上限**——JSON 文件、JSONL 行、Parquet 文件、读取范围和总字节数都有固定限制；结果会指出数据不完整。
- **向量输出有上限**——特征形状和统计数据最多包含 16 个值，结果会将截断标为不完整。
- **episode 统计数据按 episode 提供**——仅当 `episode_index` 选中的 v3.0 行包含统计数据时，结果才会返回这些数据。

<a id="dev-note"></a>
### 开发备注

<details>
<summary>维护者上下文——点击展开</summary>

无。

</details>
