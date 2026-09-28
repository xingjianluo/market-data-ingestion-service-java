# 本地行情接入与聚合服务（Market Data Ingestion Service）

在 Spring Boot 3.4 / Java 21 骨架上实现的一套**本地模拟**行情接入、聚合与重放恢复服务。
不依赖任何外部消息中间件：行情源由 `LocalMarketDataSource` 以确定性脚本模拟，
状态以本地 JSON 文件持久化，覆盖「接入 → 归一化 → 序号判定 → 聚合 → 位点提交 →
重启恢复 / 回退重放」整条链路。

目标：面对**乱序到达、重复投递、同序号矛盾、并发写入、重启恢复、位点回退**，
对每一条输入都给出**一致且可解释**的结论，且聚合结果确定、不重复累加、不丢失。

---

## 1. 接入协议

### 1.1 原始报文

`POST /api/marketdata/ingest`，`Content-Type: application/json`：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `sourceId` | string | 行情源标识。**序号按源独立编号**，去重键为 `(sourceId, sequence)` |
| `sequence` | long | 源内单调递增序号，从 1 开始，不允许跳号到达 |
| `type` | string | `TRADE`（逐笔成交）或 `QUOTE`（报价） |
| `symbol` | string | 标的代码，必须在本地注册表内 |
| `eventTime` | string | ISO-8601 UTC 事件时间，例如 `2026-09-28T01:00:00.150Z` |
| `price` | string | 成交价 / 报价价格，正数，内部以 `BigDecimal` 处理 |
| `size` | string | 成交量 / 报价量，正数 |
| `side` | string | `TRADE`：`BUY`/`SELL`；`QUOTE`：`BID`/`ASK` |

### 1.2 接入结论（响应体）

业务上的拒绝/重复**仍返回 HTTP 200**，结论看响应体 `status`，共三种：

```json
{
  "status": "ACCEPTED | REJECTED | DUPLICATE",
  "flag": "ACCEPTED | ACCEPTED_LATE_IN_WINDOW | null",
  "rejectReason": "见下表，ACCEPTED 时为 null",
  "duplicateReason": "见下表，非 DUPLICATE 时为 null",
  "message": "中文判定依据",
  "normalized": { "...": "归一化后的内部模型" },
  "symbol": "600519.SH"
}
```

**拒绝原因 `rejectReason`（彼此可区分）：**

| 原因 | 触发条件 |
| --- | --- |
| `MISSING_FIELD` | sourceId/序号/类型/标的/方向/时间等必填项缺失或空白，序号 ≤ 0 |
| `INVALID_PAYLOAD` | `type` 取值不是 TRADE/QUOTE |
| `CONTRADICTORY_RECORD` | 单条内类型与方向矛盾（TRADE+BID、QUOTE+BUY 等），或**同序号已接受过但载荷不同** |
| `UNKNOWN_SYMBOL` | 标的不在注册表（当前：`600519.SH` `000001.SZ` `AAPL.US` `BTC-USD`） |
| `INVALID_TIMESTAMP` | 时间戳无法解析、早于 2000-01-01、或领先当前时钟超过 60s（容忍时钟漂移） |
| `INVALID_NUMBER` | 价格/数量不可解析或 ≤ 0 |
| `SEQUENCE_GAP` | 该源 `sequence > lastContiguous+1`，中间序号尚未到达（不跳号、不静默丢弃） |
| `LATE_OUT_OF_WINDOW` | 事件时间落后**同源**已接受最大事件时间超过迟到窗口 |
| `OFFSET_ROLLBACK` | 重放起点回退到已提交位点（只在重放起点结论中出现） |

**重复原因 `duplicateReason`：**

| 原因 | 触发条件 |
| --- | --- |
| `SAME_SEQUENCE_SAME_PAYLOAD` | `(sourceId, sequence)` 已接受且归一化载荷指纹完全一致（at-least-once 重发） |
| `ALREADY_COMMITTED_OFFSET` | 重放帧的位点 ≤ 该源已提交位点 |

> 同序号矛盾按「拒绝」处理（`REJECTED/CONTRADICTORY_RECORD`），因为无法判断哪份载荷正确；
> 同序号完全一致按「重复」处理（`DUPLICATE`），安全跳过。两者结论可区分。

### 1.3 其他接口

| 方法/路径 | 说明 |
| --- | --- |
| `POST /api/marketdata/replay?scenario={name}&fromOffset={n}` | 重放本地脚本 |
| `GET /api/marketdata/aggregates` | 全部标的聚合视图 |
| `GET /api/marketdata/aggregates/{symbol}` | 单标的聚合视图 |
| `GET /api/marketdata/sources` | 全部行情源的序号/缺口/位点状态 |
| `GET /api/marketdata/sources/{sourceId}` | 单源状态（含 `missingSequences`、`gapPending`） |

---

## 2. 聚合规则

按标的维护一份聚合状态（成交量、成交额、最高/最低/最新价、最新 BID/ASK、VWAP、
接受/重复/拒绝计数等）。

1. **并发**：每个标的一把锁，标的内更新串行化、跨标的互不阻塞；
   去重集合为并发结构。并发混合乱序提交的结果与单线程顺序提交完全一致。
2. **去重**：以 `(sourceId, sequence)` 为幂等键；重复帧永不二次计价。
   序号跟踪器与聚合器各做一层去重（双保险）。
3. **定序**：「最新成交价 / 最新 BID / 最新 ASK」严格按比较键
   `(eventTime, sourceId, sequence)` 取最新，**与物理到达顺序无关**；
   同一事件时间用 `sourceId + sequence` 打破平局，保证确定。
4. **迟到**：迟到是**按源**的概念——用「该源在该标的上的已接受最大事件时间」做水位，
   避免不同行情源因时钟/节奏差异互相误判：
   - 落后水位 `0 < lag ≤ marketdata.late-window-millis`（默认 2000ms）：
     接受并标记 `ACCEPTED_LATE_IN_WINDOW`，仍参与聚合，VWAP/高低值自然归位；
   - 落后水位 `lag > 窗口`：拒绝 `LATE_OUT_OF_WINDOW`，**不混入结果**，计数计入 rejected。
5. **统计**：`VWAP = 成交总额 / 成交总量`（8 位小数，HALF_UP）；
   最高/最低只统计 TRADE。全部金额使用 `BigDecimal`，避免浮点误差。

---

## 3. 重放与重启恢复规则

### 3.1 序号状态机（每源独立，检查与占用原子完成）

- `seq == lastContiguous + 1` → **NEW**，原子占用并推进连续水位；
- `seq > lastContiguous + 1` → **GAP**，拒绝并在 `missingSequences` 中列出缺口；
  缺口帧**不占用**序号，待中间序号补齐后源侧重传即可被接受；
- `seq ≤ lastContiguous` → 已处理过：载荷指纹一致为**完全重复**，不一致为**矛盾**。

载荷指纹 = `type|symbol|eventTime|price(去尾零)|size(去尾零)|side`。

### 3.2 位点（offset）与快照

- 本地脚本的帧下标即投递位点 offset。重放走 `ingestAt(raw, offset)`，
  每处理一帧就把该源的已提交位点推进到该 offset，并同步落盘快照。
- 快照（`marketdata.state-file`，默认 `data/marketdata-state.json`）包含：
  每源的连续水位、已接受序号集合、载荷指纹、各计数、已提交位点；
  每标的的全部聚合数值、最新价定序键、每源事件时间水位、已应用幂等键集合。
- 写入采用「临时文件 + 原子 move」，避免半截快照。

### 3.3 三种恢复情形

| 情形 | 行为 | 对外结论 |
| --- | --- | --- |
| 同批数据重复投递（at-least-once） | 序号/聚合双层去重 | `DUPLICATE / SAME_SEQUENCE_SAME_PAYLOAD`，数值不变 |
| 重启后继续 | 启动时从 JSON 恢复聚合与序号/位点状态，新帧正常聚合 | 结果与未重启一致，不丢不重 |
| 重启后从头/回退重放 | 起点标记为 `OFFSET_ROLLBACK`；已提交位点的帧全部判重跳过，只有新帧才累加 | 起点 `verdict=OFFSET_ROLLBACK`，帧 `DUPLICATE / ALREADY_COMMITTED_OFFSET`；成交量不翻倍 |
| 跳过位点重放 | 缺失序号到达即报缺口 | 帧 `REJECTED / SEQUENCE_GAP`，与位点回退原因可区分 |

---

## 4. 并发与时序假设

- **单 JVM、本地文件**：聚合状态与序号状态为进程内内存态，持久化仅用于进程重启恢复；
  不面向多实例水平扩展（多实例需要外置存储与分布式协调，不在本工程范围）。
- **单源序号单写者语义**：同一 `sourceId` 的序号由同一投递方产生，保证「不重号」；
  服务侧保证并发写入下的判定原子性与结果确定性（`acceptIfNew` 在每源锁内完成检查+占用）。
- **at-least-once 投递**：允许重复投递，靠序号幂等去重；不依赖输入有序。
- **事件时间 vs 处理时间**：聚合以事件时间定序；迟到窗口以「同源已接受最大事件时间」
  为基准，而非系统时间，因此脚本可回放、测试可重复。
- **时钟**：归一化用可注入的 `java.time.Clock`（Bean），生产为系统 UTC，
  测试用 `Clock.fixed`；事件时间领先当前时钟超过 60s 视为非法。
- **持久化粒度**：当前为每处理一帧同步落盘（本地演示足够，崩溃窗口最小）；
  高吞吐场景应改为批量/异步提交，不影响判定语义。

---

## 5. 本地验证方法

### 5.1 构建与测试

```bash
mvn test                 # 全量单测（当前 49 个），乱序/重复/并发/恢复均覆盖
mvn package              # 打包
mvn spring-boot:run      # 启动（默认 8080；application.properties 可改为 0 随机端口）
```

单测以断言 + 日志双重呈现：每个测试都会打印**输入报文、判定结论、原因与中文依据**。
关键测试类：

| 测试类 | 覆盖重点 |
| --- | --- |
| `MarketDataNormalizerTests` | 正常路径 + 全部拒绝原因；固定时钟验证时间戳边界 |
| `MarketDataAggregatorTests` | 乱序=正序结果、重复只计价一次、窗口内/外迟到、报价按事件时间定序、**20 轮 8 线程并发写入**、并发重复投递 |
| `SourceSequenceTrackerTests` | 缺口检测与补齐、完全重复 vs 同序号矛盾、多源独立、位点单调 |
| `IngestionServiceIntegrationTests` | 全链路：乱序/重复/矛盾/非法/迟到/整批重跑幂等 |
| `ReplayRecoveryTests` | 重启快照恢复、重启后重放不翻倍、部分位点回退、跳位点缺口 |
| `ScenarioScriptTests` | 锁定确定性脚本的逐帧判定序列，防回归 |
| `WebApiIntegrationTests` | REST 黑盒：ACCEPTED/REJECTED/DUPLICATE 与查询接口 |

### 5.2 一键本地演示（无需构造请求）

```bash
mvn spring-boot:run \
  -Dspring-boot.run.arguments="--marketdata.demo-enabled=true --marketdata.state-file=data/demo.json"
```

启动后自动顺序投递 `ordered / out-of-order-dup / gap / invalid` 四个确定性脚本，
日志逐条打印输入与判定依据，最后打印各标的聚合结果。每个场景使用独立源前缀，
互不干扰。删除 `data/demo.json` 后再启动即为全新状态。

### 5.3 手工 curl

```bash
# 正常接受
curl -s -XPOST localhost:8080/api/marketdata/ingest -H 'Content-Type: application/json' -d '{
  "sourceId":"SIM-A","sequence":1,"type":"TRADE","symbol":"600519.SH",
  "eventTime":"2026-09-28T01:00:00Z","price":"100.00","size":"10","side":"BUY"}'

# 再发一遍同报文 -> DUPLICATE / SAME_SEQUENCE_SAME_PAYLOAD
# 改价格再发同序号 -> REJECTED / CONTRADICTORY_RECORD

# 查询
curl -s localhost:8080/api/marketdata/aggregates/600519.SH
curl -s localhost:8080/api/marketdata/sources/SIM-A

# 重放内置脚本（fromOffset 小于已提交位点会得到 OFFSET_ROLLBACK）
curl -s -XPOST 'localhost:8080/api/marketdata/replay?scenario=ordered&fromOffset=0'
```

### 5.4 配置项（`application.properties` / 命令行覆盖）

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `marketdata.late-window-millis` | `2000` | 可接受迟到窗口（毫秒） |
| `marketdata.state-file` | `data/marketdata-state.json` | 快照文件路径 |
| `marketdata.demo-enabled` | `false` | 启动时是否自动演示 |
| `server.port` | `8080` | HTTP 端口 |

---

## 6. 代码结构

```
model/       统一内部模型、原始报文、三类结论与全部原因枚举
normalize/   标的注册表、归一化与字段/时间戳/数值/矛盾校验
source/      本地模拟行情源（确定性脚本：乱序/重复/缺口/非法/迟到）
aggregate/   按标的并发聚合、事件时间定序、按源迟到窗口判定
replay/      序号状态机、JSON 快照持久化、重放协调（位点回退/缺口）
service/     接入门面：归一化 → 序号 → 聚合 → 位点 → 持久化
web/         REST 接口
config/      Clock、配置属性
bootstrap/   本地演示启动器
```
