---
sidebar_position: 5
title: Tables
description: Table definitions and mutations (UPDATE, DELETE, UPSERT)
---

# Tables

Tables in EventFlux store reference data that can be joined with streams and mutated via stream-triggered operations.

## Table Definition

### CREATE TABLE

Create a table with a schema:

```sql
CREATE TABLE TableName (
    attribute1 TYPE,
    attribute2 TYPE,
    ...
) WITH (extension = 'inMemory');
```

**Example:**

```sql
CREATE TABLE StockTable (
    symbol STRING,
    price DOUBLE,
    volume INT
) WITH (extension = 'inMemory');
```

### Table Extensions

Tables support different storage backends via the `extension` property:

| Extension | Description |
|-----------|-------------|
| `inMemory` | In-memory storage (default) |
| `jdbc` | JDBC-connected database |
| `cache` | Cached table with TTL support |

## Table Mutations

EventFlux supports stream-triggered table mutations: operations that modify table data based on incoming stream events.

### UPDATE

Update table rows when a stream event matches a condition:

```sql
UPDATE TableName SET column = expression
FROM SourceStream
WHERE TableName.key = SourceStream.key;
```

**Semantics:**
- Each event from `SourceStream` triggers an UPDATE
- Only rows matching the WHERE condition are updated
- SET clause can reference both table and stream columns

**Example:**

```sql
CREATE TABLE StockTable (symbol STRING, price DOUBLE, volume INT)
    WITH (extension = 'inMemory');
CREATE STREAM UpdateStream (symbol STRING, newPrice DOUBLE);

-- Update price when symbol matches
UPDATE StockTable SET price = UpdateStream.newPrice
FROM UpdateStream
WHERE StockTable.symbol = UpdateStream.symbol;
```

**Multiple Column Update:**

```sql
UPDATE StockTable
SET price = UpdateStream.newPrice, volume = UpdateStream.newVolume
FROM UpdateStream
WHERE StockTable.symbol = UpdateStream.symbol;
```

**Update with Expression:**

```sql
UPDATE StockTable SET price = UpdateStream.multiplier * 100.0
FROM UpdateStream
WHERE StockTable.symbol = UpdateStream.symbol;
```

### DELETE

Delete table rows when a stream event matches a condition:

```sql
DELETE FROM TableName
USING SourceStream
WHERE TableName.key = SourceStream.key;
```

**Semantics:**
- Each event from `SourceStream` triggers a DELETE
- All rows matching the WHERE condition are removed

**Example:**

```sql
CREATE TABLE StockTable (symbol STRING, price DOUBLE, volume INT)
    WITH (extension = 'inMemory');
CREATE STREAM DeleteStream (symbol STRING);

-- Delete row when symbol matches
DELETE FROM StockTable
USING DeleteStream
WHERE StockTable.symbol = DeleteStream.symbol;
```

### UPSERT (UPDATE or INSERT)

Insert a new row or update an existing row based on a matching condition:

```sql
UPSERT INTO TableName
SELECT column1, column2, ...
FROM SourceStream
ON TableName.key = SourceStream.key;
```

**Semantics:**
1. Evaluate the ON condition against each table row
2. If a matching row exists: UPDATE that row with stream values
3. If no match: INSERT the stream event as a new row

**Example:**

```sql
CREATE TABLE StockTable (symbol STRING, price DOUBLE, volume INT)
    WITH (extension = 'inMemory');
CREATE STREAM StockStream (symbol STRING, price DOUBLE, volume INT);

-- Insert new stock or update existing
UPSERT INTO StockTable
SELECT symbol, price, volume
FROM StockStream
ON StockTable.symbol = StockStream.symbol;
```

**Advantages over PostgreSQL's ON CONFLICT:**
- Supports any boolean expression, not just primary key columns
- Can express complex conditions like `ON StockTable.symbol = stream.symbol AND StockTable.price < stream.price`
- More natural for stream processing use cases

## Inserting into Tables

### INSERT via Query

Insert stream events into a table using a standard query:

```sql
CREATE TABLE StockTable (symbol STRING, price DOUBLE, volume INT)
    WITH (extension = 'inMemory');
CREATE STREAM StockStream (symbol STRING, price DOUBLE, volume INT);

-- Insert all events from stream
SELECT symbol, price, volume
FROM StockStream
INSERT INTO StockTable;
```

### INSERT from Window

Insert aggregated results into a table:

```sql
SELECT symbol, AVG(price) AS avgPrice, SUM(volume) AS totalVolume
FROM StockStream
WINDOW TUMBLING(1 min)
GROUP BY symbol
INSERT INTO StockSummary;
```

## Querying Tables

Tables cannot be queried directly as a stream source. They must be joined with a stream:

```sql
-- INVALID: Direct table query
SELECT * FROM StockTable INSERT INTO Output;

-- VALID: Join stream with table
SELECT s.symbol, s.price, t.companyName
FROM TradeStream AS s
JOIN StockTable AS t ON s.symbol = t.symbol
INSERT INTO EnrichedTrades;
```

See [Joins](/docs/sql-reference/joins) for more on stream-table joins.

## Best Practices

:::tip Table Mutation Tips

1. **Use specific WHERE conditions** - Avoid updating/deleting more rows than intended
2. **Consider UPSERT for idempotency** - UPSERT ensures consistent state regardless of event order
3. **Monitor table size** - In-memory tables consume heap memory; use periodic cleanup for long-running apps
4. **Test mutations thoroughly** - Verify edge cases like no-match updates and multiple-match deletes

:::

## Complete Example

```sql
-- Define streams and table
CREATE STREAM StockStream (symbol STRING, price DOUBLE, volume INT);
CREATE STREAM UpdateStream (symbol STRING, newPrice DOUBLE);
CREATE STREAM DeleteStream (symbol STRING);
CREATE TABLE StockTable (symbol STRING, price DOUBLE, volume INT)
    WITH (extension = 'inMemory');

-- Insert new stocks
SELECT symbol, price, volume
FROM StockStream
INSERT INTO StockTable;

-- Update prices from update stream
UPDATE StockTable SET price = UpdateStream.newPrice
FROM UpdateStream
WHERE StockTable.symbol = UpdateStream.symbol;

-- Delete stocks from delete stream
DELETE FROM StockTable
USING DeleteStream
WHERE StockTable.symbol = DeleteStream.symbol;

-- Or use UPSERT for insert-or-update semantics
UPSERT INTO StockTable
SELECT symbol, price, volume
FROM StockStream
ON StockTable.symbol = StockStream.symbol;
```

## Next Steps

- **[Joins](/docs/sql-reference/joins)** - Stream-to-table joins for enrichment
- **[Windows](/docs/sql-reference/windows)** - Windowed aggregations before table insert
- **[Triggers](/docs/sql-reference/triggers)** - Periodic event generation
