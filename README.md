# 本地行情接入与聚合服务（Market Data Ingestion Service）

在 Spring Boot 3.4 / Java 21 骨架上实现的一套**本地模拟行情接入 → 归一化校验 → 按标的聚合 → 仅追加日志 → 重启重放恢复**链路。
目标是：面对不同到达顺序、重复投递、矛盾数据与重启重放，对每条行情都给出**一致且可解释**的结论——
明确区分「已接受 ACCEPTED」「已拒绝 REJECTED」「已重复 DUPLICATE」，且失败原因可逐条区分、绝不静默丢弃。

---

## 1. 快速开始

```bash
# 编译 + 跑全部单测（20 个用例）
mvn test

# 打包并以固定端口启动（默认 server.port=0 随机端口）
mvn -DskipTests package
java -jar target/market-data-ingestion-service-java-1.0-SNAPSHOT.jar \
     --server.port=8081 \
     --market-data.journal-path=/tmp/md-journal.jsonl
```

启动后可用内置的本地模拟场景一键验证：

```bash
curl -s http://localhost:8081/api/simulate/scenarios
curl -X POST http://localhost:8081/api/simulate/out-of-order
curl -s http://localhost:8081/api/aggregates
curl -s http://localhost:8081/api/aggregates/BTC-USD
```

---

## 2. 接入协议（HTTP + JSON）

| 方法 & 路径 | 说明 |
|---|---|
| `POST /api/events` | 接入单条行情，返回判定结论 |
| `POST /api/events/batch` | 接入一批行情（按数组顺序处理），用于批量重放 |
| `POST /api/simulate/{场景名}` | 投递一个本地模拟场景 |
| `GET  /api/simulate/scenarios` | 列出全部内置场景 |
| `GET  /api/aggregates` | 全部标的的聚合快照 |
| `GET  /api/aggregates/{symbol}` | 单标的聚合快照 |

### 2.1 行情报文 `RawMarketEvent`

| 字段 | 类型 | 含义 |
|---|---|---|
| `sourceId` | string | 行情源标识；位点按源独立编号 |
| `offset` | long | **源内单调位点**（重放/去重/回退检测用） |
| `symbol` | string | 标的代码，须在白名单内 |
| `sequence` | long | **标的内单调序号**（聚合顺序/去重用） |
| `type` | string | `TRADE`（逐笔成交）或 `QUOTE`（报价），大小写不敏感 |
| `eventTime` | long | 事件时间，epoch 毫秒 |
| `price` / `size` | double | 成交价 / 成交量（TRADE） |
| `bidPrice`/`bidSize`/`askPrice`/`askSize` | double | 买一价/量、卖一价/量（QUOTE） |

TRADE 与 QUOTE 归一到同一个内部模型 `NormalizedEvent`，并额外记录 `receivedTime`（接收时间）。

### 2.2 返回结论 `IngestionOutcome`

```json
{
  "status": "REJECTED",
  "reason": "LATE_OUTSIDE_WINDOW",
  "sourceId": "local-feed-late", "offset": 4,
  "symbol": "MATIC-USD", "sequence": 4, "eventTime": 1790595438000,
  "detail": "迟到 29990ms 超过窗口 5000ms, watermark=..., eventTime=..."
}
```

- `status`：`ACCEPTED` / `REJECTED` / `DUPLICATE`
- `reason`：唯一、可区分的原因枚举（见下表）
- `detail`：人可读的判定依据（含期望值、实际值、水位等）

### 2.3 拒绝 / 重复原因枚举

| 原因 | 触发条件 | 结论 |
|---|---|---|
| `MISSING_SOURCE_ID` / `MISSING_SYMBOL` / `MISSING_OFFSET` / `MISSING_SEQUENCE` / `MISSING_EVENT_TIME` | 必填字段缺失或空白 | REJECTED |
| `UNSUPPORTED_TYPE` | type 缺失或不是 TRADE/QUOTE | REJECTED |
| `INVALID_TIMESTAMP` | eventTime ≤ 0，或晚于接收时间超过 60s 容忍 | REJECTED |
| `UNKNOWN_SYMBOL` | 标的不在白名单 | REJECTED |
| `MISSING_TRADE_FIELDS` / `MISSING_QUOTE_FIELDS` | 类型相关字段不齐 | REJECTED |
| `INVALID_PRICE` / `NEGATIVE_SIZE` | 价格非有限正数、数量非有限正数 | REJECTED |
| `INCONSISTENT_QUOTE` | 买价高于卖价（内在矛盾） | REJECTED |
| `TRADE_PRICE_CONTRADICTS_QUOTE` | 成交价落在当前最优报价价差之外（与已有记录矛盾/穿价） | REJECTED |
| `SEQUENCE_GAP` | 序号/位点跳空超过缓冲宽度 | REJECTED |
| `SEQUENCE_CONFLICT` | 同序号已接受，但内容不同 | REJECTED |
| `OFFSET_ROLLBACK` | 源位点回退，或同位点被篡改重发 | REJECTED |
| `DUPLICATE_OFFSET` | 同源同位点且内容一致的重复投递 | **DUPLICATE** |
| `DUPLICATE_SEQUENCE` | 同标的同序号且内容一致的重复投递 | **DUPLICATE** |
| `LATE_OUTSIDE_WINDOW` | 事件时间早于水位超过最大迟到窗口 | REJECTED（单独计数） |

> 校验是**固定顺序、短路**的：一条输入只会得到一个原因，便于定位与解释。顺序为
> 必填字段 → 类型 → 时间戳合法性 → 标的白名单 → 类型字段完备性 → 数值合法性 →
> 报价内在一致性 → 源位点层 → 标的序号/时序层 → 穿价。

---

## 3. 聚合规则

### 3.1 两层身份、两套水位

- **源位点层（跨标的）**：身份是 `(sourceId, offset)`。每个源维护「已见位点集合 + 高水位 `maxOffset`」。
- **标的序号层（单标的）**：身份是 `(symbol, sequence)`。每个标的维护「已应用序号 + 已见签名 + 事件时间水位 `watermark`」。

### 3.2 乱序：缓冲 + 连续排空（hold & drain）

为了同时支持「窗口内乱序可接受」与「序号缺口必须可见」，标的层采用确定性的缓冲规则：

1. `sequence == lastSeq + 1`：立即应用，并顺势把缓冲区里**序号连续**的事件按序排空；
2. 序号超前、但超前距离 ≤ **缓冲宽度 16**：先 `HELD` 缓冲，等待缺号补齐；缓冲事件**不计入当前聚合结果**；
3. 序号超前 > 16：判 `SEQUENCE_GAP` 拒绝（缺口可见）；
4. 缓冲中的事件同样占用其源位点，因此补齐时不会被误判为位点回退/跳空。

源位点层使用同构的「已见集合 + 高水位 + 缓冲宽度 16」模型：

- 已见过的位点 + 相同内容 → `DUPLICATE_OFFSET`（幂等，不计价）；
- 已见过的位点 + 不同内容 → `OFFSET_ROLLBACK`（同位点篡改）；
- 未见过且超前高水位 > 16 → `SEQUENCE_GAP`；
- 未见过且低于回退下界 `max(1, maxOffset − 16)` → `OFFSET_ROLLBACK`；
- 其余窗口内乱序位点正常接收（可能在标的层被缓冲）。

### 3.3 迟到窗口

- 以「已观测到的最大事件时间」为水位；事件时间早于水位、且迟到量
  `watermark − eventTime > market-data.max-out-of-order-millis`（默认 5000ms）时，
  判 `LATE_OUTSIDE_WINDOW` **拒绝并单独计数**，绝不混入聚合结果；
- 窗口内的迟到允许接收（配合序号缓冲）。

### 3.4 聚合指标（每个标的）

最近成交价/时间、累计成交量、成交笔数、最高价、最低价、**VWAP（按成交额/量）**、
最优买卖价/量、事件时间水位，以及 `accepted / duplicate / rejected / lateRejected` 计数。

**结论只依赖事件身份、序号、位点与事件时间，与到达时刻、到达线程无关。**

---

## 4. 重放与重启恢复

- 只有 `ACCEPTED` 的事件才写入**仅追加日志**（JSON Lines，每行一条 `NormalizedEvent`），
  每条 `append` 后立即 flush；`REJECTED` / `DUPLICATE` 不写日志。
- 启动时（`ApplicationReadyEvent`）按写入顺序 `replayAll()`，通过 `AggregationService.replay(...)`
  **直接重建**内存聚合状态与源位点水位，不重复走拒绝/重复判定。
- 因此：
  - 同一批数据重复投递 → 命中已见位点/序号，返回 `DUPLICATE`，**不重复累加**；
  - 位点回退后重放脏数据 → `OFFSET_ROLLBACK`，**不污染状态**；
  - 恢复只重建「已接受」的业务聚合；历史的重复/拒绝计数不持久化（它们本就未进入日志）。
- 日志路径由 `market-data.journal-path` 指定，默认 `${java.io.tmpdir}/market-data-journal.jsonl`。

---

## 5. 并发与时序假设

- 每个标的一把 `ReentrantLock`，每个源一把对象锁，加锁顺序全局固定为「源锁 → 标的锁」，
  避免锁序反转；并发写入被串行化，**同一时刻不会重复计价或数值错乱**。
- **假设：每个源的位点由该源按序产生。** 系统保证「同一状态被并发写」的正确性，
  但不修复「源自身乱发位点」——乱发位点会被按回退/跳空明确拒绝。
- **标的序号允许在缓冲宽度（16）内乱序**；超出宽度视为缺口。
- **事件时间**用于乱序/迟到判定；`eventTime` 不允许在未来超过 60s，也不允许 ≤ 0。
- 价格/数量使用有限正数；同一笔成交必须落在当前最优报价 `[bid, ask]` 区间内。
- 并发结果与「按源按序的串行参考」逐字段一致（见 `AggregationConcurrencyTest`）。

---

## 6. 本地验证方法

### 6.1 单元 / 集成测试（`src/test/.../tests`）

| 测试类 | 覆盖重点 |
|---|---|
| `EventNormalizerTest` | 字段缺失、非法时间戳、未知标的、内在矛盾等 14 种可区分原因 |
| `SymbolAggregatorTest` | 乱序缓冲/排空、重复 vs 矛盾、缺口、窗口外迟到、穿价、计数确定性 |
| `AggregationServiceTest` | 源位点回退/重复/篡改/跳空、跨源跨标的独立、恢复幂等、乱序补齐 |
| `AggregationConcurrencyTest` | 多线程并发写入结果与串行一致；窗口内乱序并发全部恰好计价一次；超宽缺口拒绝 |
| `IngestionPipelineTest` | 全部模拟场景端到端：结论一致、原因可区分、仅 ACCEPTED 落日志、恢复一致 |
| `RecoveryIntegrationTest` | 同一日志两次“启动”状态一致；重启后重复=幂等、回退=拒绝 |
| `LocalMarketFeedTest` | 每个场景产出字段齐备的报文，未知场景显式报错 |

测试日志会打印**每条输入**与**判定结论/原因/依据**（`收到行情输入 … => 判定结论 …`）。

```bash
mvn test                                  # 全部
mvn -Dtest=AggregationConcurrencyTest test
```

### 6.2 内置模拟场景

`happy`（顺序）、`out-of-order`（1,3,2,4 乱序）、`duplicate`（同位点同内容重发）、
`contradiction`（同序号不同内容）、`offset-tamper`（同位点篡改）、
`gap`（超宽序号缺口）、`rollback`（位点回退）、`late`（窗口外迟到）、
`crossed-trade`（穿价成交）、`invalid`（各类非法字段）、`mixed`（两标的交错+重复+跳空）。

> 每个场景使用独立的源标识（`local-feed-<场景>`）与独立演示标的，因此场景之间互不占用位点，
> 可重复演示；场景内部仍共用一个源，以真实触发重复/篡改/回退。

典型期望（状态, 原因）：

```
happy          全部 ACCEPTED
out-of-order   全部 ACCEPTED（seq3 先缓冲，seq2 到后排空），最终聚合与 happy 一致
duplicate      ACCEPTED, ACCEPTED, DUPLICATE/DUPLICATE_OFFSET
contradiction  ACCEPTED, ACCEPTED, REJECTED/SEQUENCE_CONFLICT
offset-tamper  ACCEPTED, ACCEPTED, REJECTED/OFFSET_ROLLBACK
gap            ACCEPTED, ACCEPTED, REJECTED/SEQUENCE_GAP
rollback       ACCEPTED, ACCEPTED, REJECTED/OFFSET_ROLLBACK
late           ACCEPTED, ACCEPTED, ACCEPTED, REJECTED/LATE_OUTSIDE_WINDOW
crossed-trade  ACCEPTED, REJECTED/TRADE_PRICE_CONTRADICTS_QUOTE
```

### 6.3 手工验证重启恢复

```bash
# 1) 投递数据
curl -X POST http://localhost:8081/api/simulate/happy
wc -l /tmp/md-journal.jsonl          # 只有 ACCEPTED 才落盘

# 2) 重启进程（同样的 journal-path）
#    日志可见：重放事件日志: N 条 / 重放恢复完成: 共重建 N 条已接受事件

# 3) 相同报文（相同 offset 与内容）重发 -> DUPLICATE；落后位点重放 -> OFFSET_ROLLBACK
```

---

## 7. 配置项（`market-data.*`）

| 配置 | 默认值 | 含义 |
|---|---|---|
| `max-out-of-order-millis` | `5000` | 可接受的最大迟到时间窗（事件时间，毫秒） |
| `journal-path` | `${java.io.tmpdir}/market-data-journal.jsonl` | 仅追加事件日志路径 |
| `symbols` | BTC/ETH/SOL/XRP/ADA/DOGE/DOT/MATIC/LTC-USD | 已知标的白名单 |

---

## 8. 代码结构

```
model/         枚举与记录：RawMarketEvent / NormalizedEvent / IngestionOutcome /
               AggregateSnapshot / EventType / IngestionStatus / RejectReason
instrument/    InstrumentRegistry（标的白名单）
config/        MarketDataProperties
journal/       EventJournal 接口 + FileEventJournal（JSON Lines 仅追加日志）
ingest/        EventNormalizer（归一化校验）、MarketDataIngestor（管线 + 启动恢复）
aggregation/   SymbolAggregate（单标的可变状态）、SymbolAggregator（序号/时序判定+锁）、
               AggregationService（源位点层 + 按标的分发 + 恢复）
source/        LocalMarketFeed（本地模拟行情源与场景）
web/           MarketDataController（HTTP 端点）
```
