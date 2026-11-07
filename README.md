# rc_linxy — API 通知服务

> 企业内部通知中台：业务系统在关键事件发生时提交一次 HTTP 请求，本服务负责**可靠地投递**到外部供应商 API —— 不管对方返回什么格式、超时还是宕机。

## 30 秒理解

```
业务系统                本服务                                    外部供应商
─────────               ────────                                  ──────────
ads / crm / trade  ──▶  POST /api/v1/notifications  ─┐
                        ├─ 校验 + 幂等 + 落库(PENDING) ──▶ 202 + id
                        │
                        │  投递引擎（1s 扫描）
                        │    ├─ 供应商限速（令牌桶）
                        │    ├─ 模板渲染（URL/Header/Body，{{var}}）
                        │    ├─ HTTP 投递 ──────────────▶ vendor API
                        │    ├─ 2xx  → DELIVERED
                        │    ├─ 5xx/429/超时 → 退避重试（1s,5s,10s,30s,60m…）
                        │    ├─ 4xx  → DEAD（重试无意义，死信可人工重发）
                        │    └─ 每次尝试全程审计
```

核心承诺：**通知要么送达，要么进入死信等待人工处置 —— 绝不静默丢弃。**

## 快速开始

```bash
# 依赖：JDK 21+（本项目用 Temurin 26 编译验证过）、Maven 3.9+
mvn spring-boot:run          # 起服务，默认 :8080，SQLite 落在 ./data/notify.db

# 或打包运行
mvn package -DskipTests
java -jar target/notify-service-1.0.0.jar

# 完整端到端演示（含 mock 供应商：成功/重试/死信/幂等四种剧本）
./demo.sh
```

### 提交通知

```bash
curl -X POST localhost:8080/api/v1/notifications \
  -H "Authorization: Bearer dev-token-1" \
  -H "Content-Type: application/json" \
  -d '{
    "supplier":  "crm",              // 对应 suppliers/crm.yaml
    "eventType": "payment.succeeded",
    "payload":   "{\"contact_id\":\"c-9527\",\"status\":\"subscribed\",\"paid_at\":\"2026-11-07\"}",
    "dedupKey":  "order-2026-000042"  // 可选；同 source+key 幂等
  }'
# → 202 {"notification_id":"...","status":"PENDING"}
```

### 查询与运维

```bash
curl -H "Authorization: Bearer dev-token-1" localhost:8080/api/v1/notifications/{id}          # 状态
curl -H "Authorization: Bearer dev-token-1" localhost:8080/api/v1/notifications/{id}/attempts # 投递轨迹
curl -H "Authorization: Bearer dev-token-1" localhost:8080/api/v1/dead-letters                # 死信列表
curl -X POST -H "Authorization: Bearer dev-token-1" localhost:8080/api/v1/dead-letters/{id}/redeliver  # 重发
curl localhost:8080/api/v1/metrics                                                            # 指标
```

## 架构与设计决策

### 1. 同步接入、异步投递（解耦是本题第一要义）

业务系统只和本服务打交道：提交即 ACK（202 + ID），后续投递、重试、格式适配全部下沉到服务内部。业务方不关心供应商返回值 → 本服务消化全部复杂度：超时、重试、格式差异、限流。

### 2. 多供应商差异 = 声明式 YAML 模板，不是代码

每个供应商一个 `suppliers/*.yaml`：URL / Header / Body 都是 `{{payload.xxx}}` `{{secret.xxx}}` 模板。

```yaml
name: crm
request:
  url: https://crm.example.com/api/v1/contacts/status
  headers:
    Authorization: "Bearer {{secret.crm_token}}"   # 密钥来自环境变量/密钥管理，不落盘
  body: '{"contactId": "{{payload.contact_id}}"}'
retry:
  backoff_seconds: [1, 5, 10, 30, 60]   # 可覆盖全局退避
rate_limit:
  qps: 50                                # 每供应商独立令牌桶
```

**新增一个外部系统 = 加一个 YAML 文件，零代码、零发版**（运行中热加载，每 30s 重扫目录）。这正是题目"不同供应商 API 请求地址、Header/Body 格式都不同"的直接解法。

取舍说明：如果供应商有 HMAC 签名等复杂逻辑，模板无法覆盖时，可以按 mockingbird 的做法退化为"每供应商一个 Java Client 类"（模板方法模式隔离差异）。当前模板方案覆盖了纯 HTTP 差异的绝大多数场景，复杂场景留了 `Renderer` 扩展点。

### 3. 投递状态机：失败必须分类，不能笼统重试

对齐推送系统 PushStatus 的三分思想（这也是我从参考系统里学到的最重要的一点）：

| 结果 | 判定 | 动作 |
|---|---|---|
| SUCCESS | 2xx | 终态 DELIVERED |
| RETRYABLE | 429 / 5xx | 按退避梯度重试 |
| TRANSPORT_ERROR | 连接失败 / 读超时 | **状态未知，仍重试**（at-least-once） |
| NON_RETRYABLE | 4xx / 渲染失败 | 直接 DEAD（重试也不会好） |

超时是"中间状态"：对方可能已收到。选择重试（宁可重复、不可丢失），配合出站去重头 `X-Notify-Dedup-Key` 让接收方实现幂等 —— 这是 at-least-once 语义的标准配套。

### 4. 退避重试：固定梯度序列，不做无限指数

`1s, 5s, 10s, 30s, 60s, 300s, 900s`（供应商可覆盖），attempt 随通知行透传，超过 `max_attempts`（默认 8）进 DEAD。

- 借鉴 RocketMQ delayLevel：固定档位比动态指数退避更**可预测、可运维**（知道第 N 次重试在多久之后）；
- 重试耗尽不是丢弃而是死信 + 一键人工重发 —— 运营兜底优先于自动补偿；
- 限速桶满时**跳过本轮且不消耗 attempt 次数**：限流不该吃掉重试预算。

### 5. 可靠性三件套：落库先于投递、审计、幂等

- **先写库再投递**（write-ahead）：提交先持久化为 PENDING，进程崩溃重启后扫描继续投递，不丢；
- **每次出站一条审计记录**（URL、请求摘要、HTTP 状态、耗时、结果分类）—— 能回答"这条通知到底发了几次、对方回了什么"；
- **入向幂等**：`source + dedup_key` 唯一索引，业务方重试提交拿到同一 ID；**出向幂等**：去重头随请求带给供应商。

### 6. 限速：每供应商独立令牌桶

防止重试风暴打爆对方（上游挂了恢复瞬间，积压的重试会瞬间涌出）。令牌桶参数在供应商 YAML 里声明，QPS 可按供应商分级。

## 代码结构

```
src/main/java/com/linxy/notify/
├── api/            接入层：提交/查询/死信管理 + Bearer 鉴权
├── delivery/       投递引擎：扫描调度、限速、HTTP 客户端、结果分类
├── domain/         Notification 聚合（状态机）
├── infra/          存储仓库、供应商注册表（热加载）
├── supplier/       SupplierSpec（YAML→规格）、Renderer（模板渲染）、SpecLoader
├── config/         Schema 初始化
└── metrics/        运行指标
```

## 测试

```bash
mvn test
```

- **单元测试**：状态分类、模板渲染（含变量缺失、非法 JSON、模板注入防护）
- **端到端**：提交→投递→DELIVERED 全链路、dedup_key 幂等、未知供应商拒绝、未授权拒绝

## 生产化路线（明确不做的 & 为什么）

当前实现刻意保持**单人可读的体量**，以下为生产化清单而非现状：

| 项 | 现状 | 生产方案 |
|---|---|---|
| 存储 | SQLite 单文件 | MySQL/PG（schema 兼容，改 datasource 即可）+ 乐观锁抢占任务 |
| 队列 | DB 扫描（1s 轮询） | MQ 延迟消息（RocketMQ delayLevel / Kafka+时间轮），扫描做兜底 |
| 多实例 | 单实例 | DB 行级抢占或分片；限速器换 Redis 令牌桶 |
| 鉴权 | 静态 token | 内部统一认证（JWT/签名），按 source 配额与审计 |
| 密钥 | 环境变量 | Vault/KMS，按供应商命名空间隔离 |
| 可观测 | /metrics JSON + 审计表 | Prometheus + 告警（DEAD 增速、重试率、供应商成功率分维度） |
| 供应商回调 | 无 | 投递成功/死信回调业务方（webhook），消除轮询 |

## 与参考实现（xhs push 系统）的对照

本服务从推送系统的分层里吸收了什么：

| 参考（push 链路） | 本服务对应 |
|---|---|
| peregrine 接入层：HTTP 收请求即 ACK，异步化 | `POST /notifications` → 202 + 状态查询 |
| mockingbird 发送骨架：模板方法隔离厂商差异 | 声明式 YAML 模板（更进一步：零代码接入） |
| PushStatus 三分：成功/可重试/不可重试 | `Outcome.classify` 状态机 |
| delayLevel 固定档位重试 | `backoff-seconds` 梯度序列 |
| 重试耗尽降级/死信，绝不静默丢弃 | DEAD 状态 + redeliver API |
| BucketQuotaService 按桶限流 | 每供应商令牌桶 |
| 超时是中间状态 → 宁重复不丢失 | TRANSPORT_ERROR 重试 + 出站去重头 |
| msgTrace 全链路投递留痕 | `delivery_attempt` 审计表 |

## 提交物说明

- 代码：本仓库
- AI 会话记录：`ai-session.zip`（完整未删改）
- 开发环境：macOS + JDK 21（编译目标）+ Maven + Spring Boot 3.3

---
*林雪余 · AI Coding 作业 · 2026-11*
