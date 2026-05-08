## Twinkle 自建 TaaS（A 类：训练 API）阅读与改造指南（单机多卡）

本文面向“先单机多卡自部署，再按需求逐步改造”的场景，目标是把 Twinkle 的 **Tinker/Twinkle 兼容训练 API** 部署到你自己的云机器上，对外提供稳定、可控、安全的训练服务入口（类似 `base_url=https://www.modelscope.cn/twinkle` 的自建替代）。

---

### 0. 你现在要做的事（最短路径）

- **先跑通**：用一份 `server_config.yaml` 启动服务端，客户端把 `base_url` 改成你的地址，完成一次训练与 `save_state`
- **再安全**：实现真正的 API Key 校验（默认鉴权通常只是“挂载点”，并非生产可用）
- **再可控**：按租户做限流/并发/超时，避免单租户把 GPU 打爆
- **再运营**：日志/指标/反代 HTTPS/产物存储

---

### 1. 推荐阅读顺序（从外到内，最快形成全局图）

#### 1.1 入口与编排：服务是怎么启动、怎么部署多组件的

先读这两处，搞清楚“YAML 配置如何变成线上服务”：

- `src/twinkle/server/__main__.py`
  - 作用：CLI 入口，只负责解析参数、读取 config、调用 `launch_server()`
- `src/twinkle/server/launcher.py`
  - 作用：初始化 Ray、启动 Ray Serve、按 `applications` 循环部署应用
  - 你需要重点理解：
    - `ray.init(address="auto")`（单机：本地起；多机：连集群）
    - `serve.start(http_options=...)`
    - `serve.run(app, name=..., route_prefix=...)`

读完后你应该能回答：

- **我改 `server_config.yaml` 的哪些字段，就能把服务跑在我的云机上？**
- **对外暴露的是哪个端口/路由前缀？内部 app 之间怎么组织？**

#### 1.2 网关与对外 API：外部调用进入哪里、路由如何注册

把“对外接口在哪里实现”先固定下来：

- `src/twinkle/server/gateway/server.py`
  - 作用：统一网关（FastAPI + Ray Serve ingress）
  - 重点：
    - 鉴权中间件：`verify_request_token`
    - 指标中间件：`create_metrics_middleware`
    - 路由注册：`_register_tinker_routes` / `_register_twinkle_routes`
- `src/twinkle/server/gateway/tinker_gateway_handlers.py`
- `src/twinkle/server/gateway/twinkle_gateway_handlers.py`

读完后你应该能回答：

- **对外我最小需要支持哪些 endpoints（训练/保存/采样等）？**
- **每个 endpoint 最终会转发/调用到哪个内部服务？**

#### 1.3 鉴权、多租户与状态：你最可能改动的“平台化核心”

你要把“谁能调用、谁拥有资源、资源如何隔离/限制”读透：

- `src/twinkle/server/utils/validation.py`
  - 作用：鉴权/请求校验入口（建议最先改造）
  - 改造目标：把“token 是否有效”做成真实逻辑（API Key / JWT），并把 token/tenant 信息传递到 state 与队列
- `src/twinkle/server/utils/state/`
  - 作用：会话/模型/采样会话等状态管理
  - 改造目标：
    - 资源隔离（不同 API key 互相不可见）
    - 资源配额（每 key 的模型数、并发数等）

读完后你应该能回答：

- **我如何做到不同 API key 的 session/model/checkpoint 互相隔离？**
- **我在哪里加 per-tenant 的配额/上限，最合理？**

#### 1.4 训练执行面：队列/限流/worker（单机多卡稳定性关键）

单机多卡最容易被打挂的点就是“并发过高、显存爆、排队失控”，优先读：

- `src/twinkle/server/utils/task_queue/`
  - `worker.py`：GPU compute task 的执行/串行化（核心稳定器）
  - `rate_limiter.py`：RPS/TPS 等限流（平台化必备）
  - `config.py`：队列配置（超时、限制、输入 token 上限等）

读完后你应该能回答：

- **一次训练请求是直接跑还是先进队列？队列怎么按 tenant 划分/限流？**
- **单机多卡如何限制每张卡的并发，避免多个任务抢一张卡？**

---

### 2. 单机多卡自部署：建议先跑通的最小闭环

#### 2.1 先找一份可用的 `server_config.yaml`

优先从 `cookbook/client/server/**/server_config*.yaml` 里选一个与你计划的 backend 接近的配置。

你需要确认这三件事：

- **`http_options.host/port`**：对外监听（单机建议 `0.0.0.0` + 指定端口）
- **`applications`**：通常包含 `server`（gateway）、`model`、`sampler`、`processor`
- **`route_prefix`**：对外统一前缀（例如 `/api/v1`），网关一般挂在这个前缀下

#### 2.2 跑通客户端调用（最重要的验收）

跑通的定义不是“服务启动了”，而是：

- 客户端把 `base_url` 指向你的服务
- 能完成一次训练循环（fwdbwd + optim_step）
- 能 `save_state` 成功，并能在后续加载/推理链路里使用

建议直接用：

- `cookbook/client/tinker/self_host/*`（Tinker 兼容路径，最接近托管服务体验）

---

### 2.3 结合 `cookbook/client/server/transformer/server_config.yaml`：字段速读与单机多卡改法

下面按这份 YAML 逐段对照说明，你可以直接按自己的 GPU 数量与对外策略改：

#### (1) `http_options`：对外监听地址与端口

这决定了你对外提供服务的监听地址/端口（反代时通常只对外暴露 443）：

- `http_options.host`
  - 单机自部署常用：`0.0.0.0`（监听所有网卡）
- `http_options.port`
  - 例子里是 `8000`

#### (2) `proxy_location`

- 例子是 `EveryNode`（更偏多节点场景）
- 单机通常用哪个都行；你后续做多机时再重点研究它对网络路径/吞吐的影响

#### (3) `applications[server]`：统一网关（强烈建议对外只暴露这一层）

```yaml
- name: server
  route_prefix: /api/v1
  import_path: server
  args:
    server_config:
      per_token_model_limit: 3
    supported_models:
      - Qwen/Qwen3.5-4B
```

你需要重点理解/按需求修改：

- `route_prefix: /api/v1`
  - 对外 API 前缀。你给客户的 `base_url` 一般是 `https://你的域名`，路径由 client 侧拼上 `/api/v1/...`
- `server_config.per_token_model_limit`
  - **平台化关键参数**：每个 API key（token）最多能创建/持有多少个 adapter/model（多租户隔离与资源上限）
- `supported_models`
  - 用于校验/提示支持的 base model 列表（对外产品化时建议明确维护）

部署层（Ray Serve）关键字段：

- `max_ongoing_requests`
- `autoscaling_config.min_replicas/max_replicas`
  - 单机阶段一般固定 `1`
- `ray_actor_options.num_cpus`
  - 网关本身很轻，例子里是 `0.1`

#### (4) `applications[model-*]`：训练模型服务（真正消耗 GPU 的地方）

这份 YAML 的 model 服务已经写出来（不是注释），用于承载训练侧的 model worker：

```yaml
- name: models-Qwen3.5-4B
  route_prefix: /api/v1/model/Qwen/Qwen3.5-4B
  import_path: model
  args:
    model_id: "ms://Qwen/Qwen3.5-4B"
    nproc_per_node: 1
    device_group:
      ranks: 1
      device_type: cuda
    queue_config:
      rps_limit: 100
      tps_limit: 100000
```

你在“单机多卡”需要优先关注：

- `args.nproc_per_node`
  - **它决定每台机器起多少个 GPU 进程/worker**（常用于把训练切到多进程并行或多卡）
  - 单机先从 `1` 跑通；要吃满多卡再逐步增大并验证显存/通信开销
- `device_group.ranks`
  - 逻辑上“这个服务要用几张卡”。例子是 `1`
- `device_mesh.dp_size`
  - 数据并行维度的配置（单机先按最简单跑通，再调整）
- `queue_config.rps_limit/tps_limit`
  - **对外服务稳定性关键**：建议先保守（小一点），跑稳后再提

安全相关：

- `TWINKLE_TRUST_REMOTE_CODE: "0"`
  - 建议保持关闭（避免加载不可信 remote code）

#### (5) `applications[sampler-*]`：采样/推理服务（vLLM）

采样服务往往用于评测、采样、生成等：

```yaml
- name: sampler-Qwen3.5-4B
  route_prefix: /api/v1/sampler/Qwen/Qwen3.5-4B
  import_path: sampler
  args:
    sampler_type: vllm
    nproc_per_node: 2
    engine_args:
      gpu_memory_utilization: 0.5
      enable_lora: true
```

你在单机多卡要优先关注：

- `nproc_per_node`
  - 推理侧进程数，过大容易抢占资源/显存；建议从 `1` 起步
- `engine_args.gpu_memory_utilization`
  - 这是 vLLM 的显存占用比例；如果你同时要训练 + 推理，通常需要 **更保守**（比如 0.3~0.6 之间按实际调整）
- `enable_lora: true`
  - 若你需要加载训练出来的 LoRA 做评测/推理，需要开启

#### (6) `applications[processor]`：CPU 侧处理服务

processor 主要用于 CPU 处理/预处理等：

- `ncpu_proc_per_node`
- `device_group.device_type: CPU`

单机阶段一般照默认跑通即可，后续根据 CPU 核数与瓶颈调整。

---

### 2.4 单机多卡的“起步配置”建议（先稳再快）

如果你是单机 \(N\) 卡，建议用这个策略起步，然后再逐步加大并发/进程数：

- 网关 `server`：`min_replicas=max_replicas=1`
- `model`：
  - 先 `nproc_per_node=1` 跑通
  - 跑稳后再把 `device_group.ranks` / `nproc_per_node` 提升，并确保 GPU 不互相抢占
- `sampler`：
  - 如果训练期不需要推理，可先减少资源（甚至暂不部署）
  - 需要推理时，先把 `gpu_memory_utilization` 调保守，避免挤爆训练显存
- 限流：
  - 先把 `rps_limit/tps_limit` 设小一点，先验证超时/排队/失败路径是可控的

---

### 3. 基于你需求做改造：建议按优先级落地

下面以“公网可用”为目标，按优先级列出改造项。你不需要一次做完，建议每次只做一个小闭环。

#### 3.1（必须）实现真正的 API Key 鉴权

目标：

- 未携带 key：401
- key 无效：401
- key 有效：请求上下文里能够识别出 `tenant_id`

推荐最小实现：

- 在 `validation.py` 中实现：
  - 从请求头读取 `Twinkle-Authorization: Bearer <key>`（或你自定义 header）
  - 查表/配置校验（初期可用静态 allowlist；后期可接 DB/配置中心）
  - 将 `tenant_id` 写入 request state（供后续 state/队列/资源隔离使用）

#### 3.2（必须）按租户隔离资源（session/model/checkpoint）

目标：

- 不同 key 的用户看不到对方的 model/session
- checkpoint 路径/命名空间天然隔离（避免冲突与越权访问）

落地点：

- `utils/state/*`：所有“注册/获取/列举资源”的入口都应该基于 tenant 过滤

#### 3.3（必须）限流/并发/超时（防止单租户拖垮整机）

目标：

- 每 tenant 可配置：
  - 最大并发训练数
  - RPS/TPS 上限
  - 队列长度上限
  - 单请求超时、排队超时

落地点：

- `utils/task_queue/*`：对入队与执行处增加 per-tenant 的控制

#### 3.4（建议）观测：日志 + 指标

目标：

- 能从一次请求串起：网关→内部服务→队列→执行→结果（至少有 request_id）
- 关键指标能看到：QPS、P50/P95、队列长度、失败率、OOM/超时次数

落地点：

- gateway 的 metrics middleware 已存在，可扩展标签（tenant、endpoint、status）
- 训练执行处补齐关键事件日志（不要打印敏感信息）

#### 3.5（建议）对外部署：反向代理 + HTTPS

目标：

- 对外只暴露一个域名（建议只暴露网关端口）
- 证书自动续期
- Ray 内部端口不暴露公网（仅内网/本机访问）

实现选择：

- Caddy（省心）或 Nginx（成熟）

---

### 4. 你每次改造的“提交策略”（建议）

为了避免一次改太多导致难以回滚，建议按以下节奏提交：

- 第一次提交：只加/改部署文档与自部署配置（能跑通）
- 第二次提交：实现 API Key 校验（最小可用）
- 第三次提交：加 per-tenant 配额/限流/超时
- 第四次提交：观测与反代部署脚本（可选）

---

### 5. 常见坑（单机多卡最容易踩）

- **鉴权没做真校验就上公网**：任何人都能占满你的 GPU
- **没有 per-tenant 并发限制**：一个用户的重试/并发会拖垮全局
- **GPU 资源声明不清晰**：多个 replica 可能抢同一张卡（需要明确 `num_gpus`/并发）
- **产物只写本地盘**：重启/磁盘满会直接造成服务不可用（建议对象存储）

