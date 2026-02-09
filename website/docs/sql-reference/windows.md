---
sidebar_position: 2
title: Windows
description: Time-based, count-based, key-based, and statistical window operations in EventFlux
---

# Windows

Windows are fundamental to stream processing, allowing you to group events for aggregation and analysis. EventFlux supports **14 window types** to cover different streaming scenarios.

## Window Syntax

```sql
SELECT ...
FROM StreamName
WINDOW('windowType', parameters)
GROUP BY column
INSERT INTO Output;
```

**Time Units:** Windows use readable time units: `MILLISECONDS`, `SECONDS`, `MINUTES`, `HOURS`, `DAYS`, `WEEKS`

## Window Types Overview

| Window | Type | Description | Use Case |
|--------|------|-------------|----------|
| **Tumbling** | Time | Fixed, non-overlapping | Hourly/daily reports |
| **Sliding** | Time | Overlapping with slide | Moving averages |
| **Session** | Time | Gap-based | User sessions |
| **Time** | Time | Continuous rolling | Real-time monitoring |
| **TimeBatch** | Time | Periodic batches | Scheduled snapshots |
| **ExternalTime** | Time | Event timestamp | Out-of-order events |
| **Length** | Count | Last N events | Recent history |
| **LengthBatch** | Count | N-event batches | Batch processing |
| **Delay** | Time | Delayed emission | Late arrival handling |
| **Unique** | Key | Latest per unique key | Deduplication (keep latest) |
| **FirstUnique** | Key | First per unique key | Deduplication (keep first) |
| **Expression** | Count | Expression-based size | Dynamic count limits |
| **Frequent** | Statistical | Most frequent events | Top-N analysis |
| **LossyFrequent** | Statistical | Approximate frequency | High-volume frequency estimation |

---

## Time-Based Windows

### Tumbling Window

Non-overlapping, fixed-size time windows. Events are assigned to exactly one window.

```sql
-- 5-minute tumbling windows
SELECT sensor_id,
       AVG(temperature) AS avg_temp,
       COUNT(*) AS reading_count
FROM SensorReadings
WINDOW('tumbling', 5 MINUTES)
GROUP BY sensor_id
INSERT INTO FiveMinuteStats;
```

**Visual Representation:**
```
Events: ──●──●──●──●──●──●──●──●──●──●──●──●──●──▶
Windows: [────────][────────][────────][────────]
              W1        W2        W3        W4
```

### Sliding Window

Overlapping windows with configurable slide interval.

```sql
-- 10-second window, sliding every 2 seconds
SELECT symbol,
       AVG(price) AS moving_avg,
       MAX(price) AS max_price
FROM StockTrades
WINDOW('sliding', 10 SECONDS, 2 SECONDS)
GROUP BY symbol
INSERT INTO MovingAverages;
```

**Parameters:**
- First: Window size (with time unit)
- Second: Slide interval (with time unit)

**Visual Representation:**
```
Events: ──●──●──●──●──●──●──●──●──▶
Windows: [────────────]
           [────────────]
             [────────────]
               [────────────]
```

### Session Window

Groups events with gaps shorter than the timeout. Sessions end after inactivity.

```sql
-- User sessions with 30-minute timeout
SELECT user_id,
       COUNT(*) AS click_count,
       MIN(timestamp) AS session_start,
       MAX(timestamp) AS session_end
FROM ClickStream
WINDOW('session', 30 MINUTES)
GROUP BY user_id
INSERT INTO UserSessions;
```

**Use Cases:**
- User activity sessions
- Device connectivity windows
- Transaction sequences

**Visual Representation:**
```
Events: ●●●●     ●●     ●●●●●●●       ●●●●
Sessions: [──────] [─]   [─────────]   [────]
          Session1  S2      Session3     S4
```

### Time Window

Continuous rolling window based on event time.

```sql
-- Rolling 1-minute window
SELECT sensor_id,
       AVG(value) AS rolling_avg
FROM Readings
WINDOW('time', 1 MINUTE)
GROUP BY sensor_id
INSERT INTO RollingStats;
```

### TimeBatch Window

Batches events and emits at fixed intervals.

```sql
-- Emit batch every 10 seconds
SELECT symbol,
       SUM(volume) AS total_volume,
       COUNT(*) AS trade_count
FROM Trades
WINDOW('timeBatch', 10 SECONDS)
GROUP BY symbol
INSERT INTO BatchedStats;
```

### ExternalTime Window

Uses a timestamp attribute from the event for windowing (event time vs processing time).

```sql
-- Use event timestamp for windowing
SELECT device_id,
       AVG(temperature) AS avg_temp
FROM SensorData
WINDOW('externalTime', event_time, 5 MINUTES)
GROUP BY device_id
INSERT INTO Stats;
```

**Parameters:**
- First: Timestamp attribute name
- Second: Window duration (with time unit)

---

## Count-Based Windows

### Length Window

Maintains a sliding window of the last N events.

```sql
-- Keep last 100 trades per symbol
SELECT symbol,
       AVG(price) AS avg_price,
       STDDEV(price) AS price_stddev
FROM StockTrades
WINDOW('length', 100)
GROUP BY symbol
INSERT INTO RecentStats;
```

**Visual Representation:**
```
Events: 1 2 3 4 5 6 7 8 9 ...
Window:     [3 4 5 6 7]      (length=5)
               [4 5 6 7 8]
                  [5 6 7 8 9]
```

### LengthBatch Window

Collects N events, emits as batch, then resets.

```sql
-- Emit after every 50 events
SELECT symbol,
       AVG(price) AS batch_avg,
       SUM(volume) AS batch_volume
FROM Trades
WINDOW('lengthBatch', 50)
GROUP BY symbol
INSERT INTO BatchResults;
```

**Visual Representation:**
```
Events:  1 2 3 4 5 | 6 7 8 9 10 | 11 ...
Batches:   Batch 1      Batch 2
          [─────]      [───────]
```

### Expression Window

A count-based sliding window whose size is defined by an expression string. Currently supports `count() <= N` and `count() < N`.

```sql
-- Keep at most 20 events in the window
SELECT sensor_id,
       AVG(temperature) AS avg_temp,
       COUNT(*) AS reading_count
FROM SensorReadings
WINDOW('expression', 'count() <= 20')
GROUP BY sensor_id
INSERT INTO ExpressionStats;
```

**Parameters:**
- Expression string: `'count() <= N'` or `'count() < N'`

**Visual Representation:**
```
Events: 1 2 3 ... 19 20 21 22 ...
Window:     [1..20]         (count() <= 20)
               [2..21]
                  [3..22]
```

---

## Key-Based Windows

### Unique Window

Keeps only the **latest event** for each unique key value. When a new event arrives with the same key, it replaces the previous event (emitting the old one as expired).

```sql
-- Keep latest trade per symbol
SELECT symbol, price, volume
FROM StockTrades
WINDOW('unique', symbol)
INSERT INTO LatestTrades;
```

**Parameters:**
- Attribute name to use as the unique key

**Visual Representation:**
```
Events:  AAPL:150  GOOG:2800  AAPL:152  MSFT:300  AAPL:148
Window:  {AAPL:148, GOOG:2800, MSFT:300}  (latest per key)
```

:::caution Memory
The unique window grows with the number of distinct keys and is not bounded. Consider using with GROUP BY or filtered streams to limit cardinality.
:::

### FirstUnique Window

Keeps only the **first event** for each unique key value. Subsequent events with the same key are silently discarded.

```sql
-- Keep first occurrence per user
SELECT user_id, action, timestamp
FROM UserActions
WINDOW('firstUnique', user_id)
INSERT INTO FirstActions;
```

**Parameters:**
- Attribute name to use as the unique key

**Visual Representation:**
```
Events:  user1:login  user2:click  user1:click  user3:login  user2:scroll
Window:  {user1:login, user2:click, user3:login}  (first per key)
```

:::caution Memory
Like the unique window, firstUnique grows with distinct keys and is not bounded.
:::

---

## Special Windows

### Delay Window

Delays event emission by a specified duration. Useful for handling late arrivals.

```sql
-- Delay events by 30 seconds
SELECT *
FROM SensorReadings
WINDOW('delay', 30 SECONDS)
INSERT INTO DelayedReadings;
```

---

## Statistical Windows

### Frequent Window

Tracks the **most frequently occurring events** using the Misra-Gries counting algorithm. Maintains a bounded set of the top-N most frequent items, evicting the least frequent when the set is full.

```sql
-- Track the 5 most frequently traded symbols
SELECT symbol, price
FROM StockTrades
WINDOW('frequent', 5)
INSERT INTO TopSymbols;
```

With specific key attributes:

```sql
-- Track top 5 by symbol, keyed on symbol attribute only
SELECT symbol, COUNT(*) AS trade_count
FROM StockTrades
WINDOW('frequent', 5, symbol)
GROUP BY symbol
INSERT INTO FrequentSymbols;
```

**Parameters:**
- First: Number of most frequent items to track (integer)
- Optional: One or more attribute names for key generation (defaults to all attributes)

### LossyFrequent Window

Identifies events whose frequency **exceeds a support threshold** using the Lossy Counting algorithm. Suitable for high-volume streams where approximate frequency estimation is acceptable.

```sql
-- Find items appearing in more than 10% of events
SELECT symbol, price
FROM StockTrades
WINDOW('lossyFrequent', 0.1)
INSERT INTO FrequentItems;
```

With custom error bound:

```sql
-- Support threshold of 5%, error bound of 1%
SELECT symbol, price
FROM StockTrades
WINDOW('lossyFrequent', 0.05, 0.01)
INSERT INTO FrequentItems;
```

With key attributes:

```sql
-- Approximate frequency by symbol attribute
SELECT symbol, price
FROM StockTrades
WINDOW('lossyFrequent', 0.1, 0.01, symbol)
INSERT INTO FrequentItems;
```

**Parameters:**
- First: Support threshold (float, 0.0–1.0) — minimum frequency ratio to emit
- Optional second: Error bound (float, 0.0–1.0, defaults to support/10)
- Optional remaining: Attribute names for key generation (defaults to all attributes)

---

## Combining Windows with GROUP BY

Windows work naturally with GROUP BY for partitioned aggregations:

```sql
SELECT
    region,
    device_type,
    AVG(latency) AS avg_latency,
    MAX(latency) AS max_latency,
    COUNT(*) AS request_count
FROM NetworkRequests
WINDOW('tumbling', 1 MINUTE)
GROUP BY region, device_type
INSERT INTO RegionalStats;
```

## Window with HAVING

Filter aggregated results:

```sql
SELECT symbol,
       AVG(price) AS avg_price,
       COUNT(*) AS trade_count
FROM Trades
WINDOW('tumbling', 5 MINUTES)
GROUP BY symbol
HAVING COUNT(*) > 10 AND AVG(price) > 100
INSERT INTO ActiveHighValueStocks;
```

## Best Practices

:::tip Window Selection Guide

| Scenario | Recommended Window |
|----------|-------------------|
| Periodic reports (hourly/daily) | Tumbling |
| Moving averages | Sliding |
| User session analysis | Session |
| Recent event history | Length |
| Batch processing | LengthBatch or TimeBatch |
| Out-of-order events | ExternalTime |
| Late arrival handling | Delay |
| Deduplication (keep latest) | Unique |
| Deduplication (keep first) | FirstUnique |
| Top-N frequency tracking | Frequent |
| High-volume frequency estimation | LossyFrequent |
| Dynamic count-based window | Expression |

:::

:::caution Memory Considerations

- **Large windows** consume more memory
- **Session windows** can grow unbounded for active keys
- **Unique/FirstUnique windows** grow with distinct key cardinality and are unbounded
- **Frequent windows** are bounded by the configured count parameter
- **Length windows** have predictable memory usage
- Monitor memory usage in production

:::

## Next Steps

- **[Aggregations](/docs/sql-reference/aggregations)** - Aggregate functions for windows
- **[Joins](/docs/sql-reference/joins)** - Joining windowed streams
- **[Patterns](/docs/sql-reference/patterns)** - Pattern detection across windows
