# rc_linxy — API 通知服务

> 企业内部通知中台：业务系统在关键事件发生时提交一次请求（HTTP 或 MQ），本服务负责**可靠地投递**到外部供应商 API —— 不管对方返回什么格式、超时还是宕机。

## 一、通俗流程演示：一笔通知的一生

以「用户订阅付款成功 → 通知 CRM 更新 Contact 状态」为例，跟着一笔通知走完全程：

**第 1 步：业务系统提交通知（收到回执只要几毫秒）**

```bash
curl -X POST localhost:8080/api/v1/notifications \
  -H "Authorization: Bearer dev-token-1" -H "Content-Type: application/json" \
  -d '{
    "supplier":  "crm",                 # 通知谁 —— 对应 suppliers/crm.yaml
    "eventType": "payment.succeeded",   # 发生了什么
    "payload":   "{\"contact_id\":\"c-9527\",\"status\":\"subscribed\"}",  # 事件内容
    "dedupKey":  "order-2026-000042"    # 幂等键（可选）：重复提交只投一次
  }'
```

业务系统立刻收到回执，**完全不用等 CRM 响应**：

```json
{"notification_id":"bce4e0c9-78cf-...","status":"PENDING"}   # HTTP 202
```

**第 2 步：服务内部（业务系统无感知）**

```
落库(PENDING) → 发 MQ(dispatch队列) → 消费者捞出
  → 按 crm.yaml 模板渲染出 CRM 要的 Header/Body 格式
  → HTTP POST 到 CRM（带 X-Notify-Dedup-Key 去重头）
```

**第 3 步：查状态（想关心才关心）**

```bash
curl -H "Authorization: Bearer dev-token-1" \
  localhost:8080/api/v1/notifications/bce4e0c9-78cf-...
```

顺利时看到（几秒内）：

```json
{"status":"DELIVERED","attempt":1,"delivered_at":"2026-11-07T14:06:36Z", ...}
```

**那如果 CRM 挂了呢？** 看服务日志里的真实流转（`./demo.sh` 可完整复现）：

```
# CRM 返回 500 → 分类为"可重试" → 按退避梯度 1s 后 MQ 延迟消息重试
retry scheduled id=857b1df5... attempt=1 next_in=1s reason=vendor server error
# 1 秒后自动重试成功
delivered id=857b1df5... supplier=crm status=200 attempt=2
```

**如果 CRM 明确拒绝（400 参数错误）？** 重试也不会好，直接进死信等人工处理：

```
dead (non-retryable) id=ef17636f... supplier=crm status=400
# 运维一键重发（修复配置后）：
POST /api/v1/dead-letters/ef17636f.../redeliver  → {"status":"PENDING"}
```

**如果业务系统重复提交同一笔？** 幂等键兜底，两次提交拿到同一个 ID：

```json
第 1 次 → 202 {"notification_id":"bce4e0c9...","status":"PENDING"}
第 2 次 → 200 {"notification_id":"bce4e0c9...","deduplicated":true}   # 不重复投递
```

一句话总结：**通知要么送达、要么在死信里等你 —— 绝不静默丢弃，绝不阻塞业务。**

## 二、整体架构

```
业务系统A ──┐ HTTP POST
业务系统B ──┤            ┌──────────────────────────────────────┐
业务系统C ──┘            │              notify-service           │
     │                   │                                      │
     │ 各自的 MQ          │  接入层 IntakeListener（队列=业务身份） │
     └──▶ notify.intake.ads / .crm / .inventory ──▶ SubmitService│
                         │      ① 校验+幂等 → 落库PENDING(事实源)  │
                         │      ② 发 dispatch MQ（秒级触发）       │
                         │              │                        │
                         │   30s 扫描兜底（MQ丢消息时捞回）◀──┐     │
                         │              ▼                    │     │
                         │  投递引擎（MQ消费者×8 并发）         │     │
                         │   ├─ 每供应商令牌桶限速              │     │
                         │   ├─ YAML模板渲染(Header/Body)      │     │
                         │   ├─ HTTP投递 + 去重头              │     │
                         │   └─ 2xx→DELIVERED                 │     │
                         │       5xx/429/超时→延迟MQ重试 ───────┼─────┘
                         │       4xx→DEAD(死信,可人工重发)      │
                         │   每次尝试写审计表                   │
                         └──────────────────────────────────────┘
                                        │
                                        ▼
                            外部供应商 API（每家一个 YAML）
```

## 三、快速开始

```bash
# 依赖：JDK 21+、Maven 3.9+。无外部中间件 —— MQ 是内嵌 Artemis，存储是 SQLite。
mvn spring-boot:run          # 起服务 :8080

# 或打包运行
mvn package -DskipTests && java -jar target/notify-service-1.0.0.jar

# 一键端到端演示（mock 供应商四种剧本：成功/500重试/400死信/幂等去重）
./demo.sh

# 全部测试（单测 + 端到端 + 200并发压测 + MQ接入）
mvn test
```

### 两种接入方式

**方式 A：同步 HTTP**（见第 1 步示例）

**方式 B：MQ 投递**（业务系统已有 MQ 链路时，直接往自己的 intake 队列发）：

```java
// 业务系统代码：往 notify.intake.crm 发 JMS TextMessage，body 是 JSON
jms.send("notify.intake.crm", session -> session.createTextMessage("""
    {"supplier": "crm", "eventType": "payment.succeeded",
     "payload": "{\"contact_id\":\"c-9527\"}", "dedupKey": "order-42"}
    """));
```

关键设计：**队列名即业务身份**（source 归属由队列决定，不信任消息体）——防伪造，也让多业务天然隔离：某业务消息堆积不影响其他业务。

### 运维接口

```bash
GET  /api/v1/notifications/{id}           # 状态查询
GET  /api/v1/notifications/{id}/attempts  # 投递轨迹（每次出站的URL/状态码/耗时）
GET  /api/v1/dead-letters                 # 死信列表
POST /api/v1/dead-letters/{id}/redeliver  # 死信重发
GET  /api/v1/suppliers                    # 可用供应商
GET  /api/v1/metrics                      # 运行指标
```

## 四、核心设计决策

### 4.1 为什么用 MQ（异步 / 解耦 / 削峰）

对照我的参考系统（推送链路：业务 → peregrine 接入层 → MQ → mockingbird 发送层），本服务用 MQ 做三件事：

| 目的 | 本服务的用法 |
|---|---|
| **异步** | 提交线程只做"校验+落库+发消息"（毫秒级），投递在消费者线程完成。业务方从不等待外部 API |
| **解耦** | 多业务 intake 队列接入（队列=身份）；dispatch 队列隔离接入与投递 —— 两层可独立扩容/重启 |
| **削峰** | 三道闸门按序生效：① dispatch 队列天然缓冲突发流量；② 每供应商令牌桶（YAML 里配 QPS）；③ 消费者并发数（默认 8）= 投递速率上限。上游秒级 1 万条洪峰，下游依旧按供应商能接受的速度匀速投出 |

**延迟重试也走 MQ**：失败后不 sleep 等待，而是发 Artemis 延迟消息（`_AMQ_SCHED_DELIVERY`，对标 RocketMQ delayLevel），消费线程立即释放去处理下一条。这是推送系统里"为什么用延迟消息而不是线程 sleep"的同一答案：**sleep 会阻塞消费线程拖垮吞吐**。

**可靠性兜底**：MQ 不是正确性的必要条件 —— DB 才是事实源。先落库再发 MQ；MQ 消息丢了，30s 扫描器会把到期通知捞回队列。两层触发（MQ 秒级 + 扫描兜底）由消费端幂等保护（状态已终态直接跳过）。

演示环境用**内嵌 Artemis**（进程内 vm://0，零外部依赖）；生产切外部集群只改两行配置（`spring.artemis.mode=native` + `broker-url`），业务代码零改动。

### 4.2 为什么存储用 SQLite 而不是 Redis

**定位不同：这份存储是"账本"，不是"缓存"。**

通知服务的核心承诺是"绝不丢"，需要的是**事实源（source of truth）**：每笔通知的状态机、每次投递的审计、死信记录。对事实源的要求是持久化 + 事务 + 可查询，这正是关系库的活：

| 需求 | SQLite（现状） | Redis |
|---|---|---|
| 落库即持久（进程崩了不丢） | ✅ fsync | ⚠️ AOF everysec 有窗口期 |
| 幂等（source+dedup_key 唯一） | ✅ 唯一索引，并发下强一致 | ⚠️ SET NX 可行但过期即失效（TTL 后重复提交无法拦截） |
| 死信/审计可查（运营页面） | ✅ SQL | ❌ 需要另外建查询链路 |
| 单机演示零依赖 | ✅ 单文件 | ❌ 要起服务 |

选 SQLite 纯粹为了"clone 即跑"的演示体验 —— schema 是标准 SQL（见 `db/schema.sql`），生产换 MySQL/PostgreSQL 只改 datasource，这也是为什么没用任何 SQLite 方言特性。

**Redis 在这套架构里有位置，但不是存事实**：多实例部署时做共享限流（令牌桶）、跨实例去重锁、热点通知缓存 —— 这些写进了生产化表格，单机演示版不需要。

### 4.3 高并发设计

**接入侧（多业务洪峰）**：
- HTTP 接入：无状态，横向扩容即可
- MQ 接入：每业务独立队列 + 独立消费者，业务隔离
- 落库先行：唯一索引天然挡掉重复提交，写放大是 O(1)

**投递侧（保护外部供应商）**：削峰三道闸（队列缓冲 → 令牌桶 → 消费并发数），重试风暴天然被驯服 —— 上游 500 错误触发的重试洪峰也会被令牌桶整形。

**验证**：`HighConcurrencyTest` 模拟 200 线程同时提交 → 断言全部 202、全部送达、vendor 侧每个 dedup 键恰好收到一次（零重复）；三个业务同时经 MQ 接入 → source 归属正确、互不干扰。全部通过。

### 4.4 多供应商差异 = 声明式 YAML 模板

每个供应商一个 `suppliers/*.yaml`，URL/Header/Body 全是模板：

```yaml
name: crm
request:
  method: POST
  url: https://crm.example.com/api/v1/contacts/status
  headers:
    Authorization: "Bearer {{secret.crm_token}}"   # 密钥来自环境变量，不落盘
  body: '{"contactId": "{{payload.contact_id}}", "status": "{{payload.status}}"}'
retry:
  backoff_seconds: [1, 5, 10, 30, 60]   # 可覆盖全局退避
rate_limit:
  qps: 50                                # 每供应商独立令牌桶
```

**新增一个外部系统 = 加一个 YAML 文件，零代码零发版**（目录热加载，30s 内生效）。复杂签名类供应商（HMAC 等）留了 `Renderer` 扩展点可退化为 Java Client（mockingbird 模板方法模式）。

### 4.5 投递状态机：失败必须分类

| 结果 | 判定 | 动作 |
|---|---|---|
| SUCCESS | 2xx | 终态 DELIVERED |
| RETRYABLE | 429 / 5xx | 延迟 MQ 重试（退避梯度 1s→5s→10s→30s→60s→300s→900s） |
| TRANSPORT_ERROR | 连接失败/读超时 | **状态未知仍重试**（at-least-once），出站带去重头让接收方幂等 |
| NON_RETRYABLE | 4xx / 渲染失败 | 直接 DEAD（重试也不会好），人工重发兜底 |

超时是"中间状态"：对方可能已收到。选择宁可重复不可丢失，配套 `X-Notify-Dedup-Key` 头让对方可幂等 —— 与推送系统对 APNs 超时"立即换代理重试一次"的取舍同源。

## 五、测试

```bash
mvn test    # 16 个用例，全部通过
```

| 套件 | 覆盖 |
|---|---|
| UnitTest (6) | 状态分类、模板渲染（变量缺失/非法JSON/注入防护/不二次展开） |
| EndToEndTest (8) | 成功投递、500→退避→成功、400→死信仅投1次、dedup幂等、未知供应商拒绝、401、规格解析、死信重发 |
| HighConcurrencyTest (2) | 三业务 MQ 接入（source=队列名）；200 并发 HTTP 提交全部送达、vendor 侧零重复 |

## 六、代码结构

```
src/main/java/com/linxy/notify/
├── api/            接入 HTTP API + Bearer 鉴权
├── infra/          SubmitService(接入统一入口)、IntakeListener(多业务MQ)、
│                   仓库、SupplierRegistry(热加载)
├── delivery/       DispatchConsumer(MQ消费)、DeliveryEngine(状态机)、
│                   HttpDeliveryClient、RateLimiterRegistry、RetryPublisher(延迟消息)
├── supplier/       SupplierSpec、SpecLoader(YAML解析)、Renderer(模板渲染)
├── domain/         Notification 聚合
├── config/         JmsConfig(内嵌Artemis)、SchemaInitializer
└── metrics/        运行指标
```

## 七、生产化路线（明确不做的 & 为什么）

当前实现保持"单人可读"的体量，以下是生产清单而非现状：

| 项 | 现状 | 生产方案 |
|---|---|---|
| MQ | 内嵌 Artemis（vm://0） | 外部集群：`spring.artemis.mode=native`，或换 RocketMQ（延迟消息原生）|
| 存储 | SQLite 单文件 | MySQL/PG（schema 兼容），分布式锁抢占任务 |
| 多实例 | 单实例 | DB 行级抢占 + Redis 令牌桶（共享限流）+ Redis 去重锁 |
| 鉴权 | 静态 token | JWT/签名，按 source 配额隔离 |
| 密钥 | 环境变量 | Vault/KMS |
| 可观测 | /metrics + 审计表 | Prometheus 告警（DEAD 增速/重试率/供应商成功率分维度） |
| 业务回调 | 轮询查询 | 投递结果 webhook 回调业务方 |

## 八、与参考实现（xhs push 系统）的对照

| 参考（push 链路） | 本服务对应 |
|---|---|
| peregrine：多业务 MQ 接入 + HTTP 接口，收即 ACK | IntakeListener（队列=业务身份）+ POST /notifications（202） |
| 业务→接入层→MQ→发送层 分层解耦 | intake 队列 → SubmitService → dispatch 队列 → DeliveryEngine |
| mockingbird：模板方法隔离厂商差异 | 声明式 YAML 模板（更进一步：零代码接入） |
| PushStatus 三分：成功/可重试/不可重试 | `Outcome.classify` 四分类状态机 |
| delayLevel 固定档位延迟重试 | backoff-seconds 梯度 + Artemis 延迟消息 |
| 重试耗尽降级/死信，绝不静默丢弃 | DEAD 状态 + redeliver API |
| BucketQuotaService 按桶限流 | 每供应商令牌桶（YAML 声明 QPS/burst） |
| 超时=中间状态→宁重复不丢失 | TRANSPORT_ERROR 重试 + 出站去重头 |
| msgTrace 全链路投递留痕 | `delivery_attempt` 审计表 |
| MQ 消费始终 SUCCESS 防重试风暴 | 消费失败不重投，DB+扫描兜底 |

## 提交物

- 代码：本仓库（Java 21 / Spring Boot 3.3 / 内嵌 Artemis / SQLite）
- AI 会话记录：`ai-session.zip`（完整未删改）
- 环境：macOS + Temurin JDK 26（编译目标 21）

---
*林雪余 · AI Coding 作业 · 2026-11*
