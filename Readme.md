# OpenSearch to ClickHouse Migration Tool

## Purpose: One-Time Historical Data Migration

This is a **one-time migration tool** designed to migrate historical data from OpenSearch to ClickHouse **before** starting live data ingestion.

> **IMPORTANT**: This script must be run **BEFORE** starting the live ingestion pipeline. It is designed for one-time historical data migration only.

### Migration Workflow:
1. **Phase 1 (This Tool)**: Migrate all historical data from OpenSearch to ClickHouse
2. **Phase 2 (Live Ingestion)**: Use live ingestion pipeline (https://github.com/yash22091/clickhouse-siem-pipeline) for real-time data going forward

### When to Use This Tool:
- You have historical data in OpenSearch that needs to be migrated to ClickHouse
- You want to consolidate old logs before starting live ingestion
- You need to migrate months/years of historical SIEM data
- **Run this BEFORE** starting the live ingestion service

### When NOT to Use This Tool:
- For ongoing real-time data ingestion (use live ingestion pipeline instead)
- After live ingestion has already started (may cause duplicates)
- For continuous synchronization between OpenSearch and ClickHouse

---

## Features

- **One-time migration** with duplicate prevention (tracks document IDs in migration files)
- **Parallel processing** for large datasets using OpenSearch sliced scroll (1TB+ support)
- **Agent-wise tables**: Separate tables per agent (e.g., `wazuh_agent`)
- **Location-wise tables**: Special tables for configured locations (SOC1, India-DC, etc.)
- **Daily partitioning**: Tables partitioned by `ingestion_date` (like Elasticsearch daily indices)
- Dynamically creates tables using `MergeTree` engine aligned with live ingestion
- Automatically flattens nested OpenSearch documents (agent, rule, manager, data)
- Checkpoint system to resume interrupted migrations
- Progress reporting and detailed migration summary
- Dry-run mode for testing without writing data
- Batch-wise inserts with dynamic schema auto-expansion
- Filters data using `@timestamp` ranges
- Primary timestamp field: `@timestamp` with fallback hierarchy

---

## Prerequisites

### System Requirements:
- **Python 3.11 or above**
- **Run Location**: This script should be run on the **ClickHouse server itself** for optimal performance and to avoid network issues
- **Execution Mode**: Run in **screen** or **tmux** session to prevent interruption

### Access Requirements:
- **OpenSearch Access**: Read-only credentials to source OpenSearch instance
  - Host, port, username, password
  - Access to target index pattern (e.g., `siem-alerts-*`)
- **ClickHouse Access**: Write permissions to ClickHouse database
  - Host, username, password, database name
  - Ability to create tables and insert data
- **Network Access**: Outbound connectivity from ClickHouse server to OpenSearch instance

---

## Setup Instructions

### Step 0: Start Screen Session (Recommended)

**Always run this migration in a screen or tmux session** to prevent interruption:

```bash
# Start a new screen session
screen -S opensearch_migration

# Or use tmux
tmux new -s opensearch_migration
```

> **Tip**: If connection drops, reattach with:
> - Screen: `screen -r opensearch_migration`
> - Tmux: `tmux attach -t opensearch_migration`

### Step 1: Prepare Environment

```bash
# SSH to ClickHouse server first
ssh user@clickhouse-server

# Create project directory
cd /root/opensearch_to_clickhouse
python3 -m venv venv
source venv/bin/activate
```

### Step 2: Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 3: Configure `.env`

Create a `.env` file in the project root:

```ini
# OpenSearch Configuration
OPENSEARCH_HOST=your-opensearch-host
OPENSEARCH_PORT=9200
OPENSEARCH_USER=admin
OPENSEARCH_PASS=your-password
OPENSEARCH_INDEX=siem-alerts-*

# ClickHouse Configuration
CLICKHOUSE_HOST=your-clickhouse-host
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=default

# Migration Settings
SPECIAL_LOCATIONS=SOC1,India-DC
BATCH_SIZE=1000
SCROLL_SIZE=5000
PARALLEL_WORKERS=1

# Performance Tuning (Optional)
CHECKPOINT_INTERVAL=10000
USE_BLOOM_FILTER=true
```

**Configuration Parameters:**
- `BATCH_SIZE`: Number of documents to batch before inserting into ClickHouse (default: 1000)
- `SCROLL_SIZE`: Number of documents per OpenSearch scroll request (default: 5000, max: 10000)
- `PARALLEL_WORKERS`: Number of parallel workers for processing large datasets (default: 1)
- `CHECKPOINT_INTERVAL`: How often to save checkpoints and migrated IDs (default: 10000 documents)
- `USE_BLOOM_FILTER`: Use memory-efficient bloom filter for duplicate detection (default: true, recommended for 10M+ documents)

> **OpenSearch Limitation**: OpenSearch has a default max result window of 10,000. The tool automatically caps `SCROLL_SIZE` at 10,000 to prevent query failures.

> **Parallel Processing**: For large datasets (1TB+), use `PARALLEL_WORKERS` to enable parallel processing using OpenSearch's sliced scroll feature. Each worker processes a different slice of data simultaneously.

> **Performance Tip**: For migrations with 100M+ documents, enable `USE_BLOOM_FILTER=true` to reduce memory usage from gigabytes to megabytes while checking for duplicates.

---

## Running the Migration

> **Before Starting**: Ensure you are inside a screen/tmux session and on the ClickHouse server

### Step 1: Verify Connectivity

```bash
# Test OpenSearch connection
curl -k -u "$OPENSEARCH_USER:$OPENSEARCH_PASS" "https://$OPENSEARCH_HOST:$OPENSEARCH_PORT/_cat/indices?v"

# Test ClickHouse connection
echo "SELECT 1" | clickhouse-client --host=$CLICKHOUSE_HOST --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD
```

### Step 2: Test with Dry Run (Recommended)

```bash
python opensearch_to_clickhouse.py --dry-run
```

### Step 3: Run Full Migration

**Inside your screen/tmux session:**

```bash
# Migrate all historical data
python opensearch_to_clickhouse.py

# Or migrate specific time range
python opensearch_to_clickhouse.py --from "2024-01-01T00:00:00Z" --to "2024-12-31T23:59:59Z"

# For large datasets (1TB+), use parallel workers
python opensearch_to_clickhouse.py --workers 4

# Combine time range and parallel workers
python opensearch_to_clickhouse.py --from "2024-01-01T00:00:00Z" --to "2024-12-31T23:59:59Z" --workers 8
```

> **Performance Tip**: For datasets over 1TB or indices with 20-30GB+ each, use `--workers` option. Start with 4-8 workers and increase based on server resources.

### Step 4: Monitor Progress

**In another terminal/screen window**, monitor the migration:

```bash
# Watch activity log
tail -f activity.log

# Check progress in real-time
watch -n 5 'tail -n 20 activity.log'
```

### Step 5: Verify Migration

Check the generated files:
- `migration_summary.json` - Migration statistics and results
- `migration_checkpoints.json` - Progress checkpoints per table
- `migrated_document_ids.json` - Tracked document IDs to prevent duplicates
- `activity.log` - Detailed migration logs

### Step 6: Detach from Screen and Verify

```bash
# Detach from screen (Ctrl+A, then D)
# Or from tmux (Ctrl+B, then D)

# Verify migration is running
screen -ls  # or: tmux ls

# Reattach if needed
screen -r opensearch_migration  # or: tmux attach -t opensearch_migration
```

### Step 7: Start Live Ingestion Pipeline

**After migration completes successfully**, deploy the live ingestion pipeline:

```bash
# Clone live ingestion repository
git clone https://github.com/yash22091/clickhouse-siem-pipeline.git
cd clickhouse-siem-pipeline

# Follow setup instructions in that repository
# The live ingestion pipeline will handle real-time data going forward
```

> **Important**: The live ingestion pipeline should only be started AFTER this one-time migration is complete to avoid data duplication or conflicts.

---

## Additional Commands

### Reset and Re-run Migration:

```bash
python opensearch_to_clickhouse.py --reset-checkpoints
```

### Resume Interrupted Migration:

Simply re-run the command - it will automatically resume from last checkpoint:
```bash
python opensearch_to_clickhouse.py
```

---

## How It Works

1. **Connects** to OpenSearch and ClickHouse
2. **Queries** OpenSearch using `@timestamp` range filters
3. **Scans** documents and tracks `_id` to prevent duplicates
4. **Processes** each document:
   - Extracts agent name and location
   - Flattens nested structures (agent, rule, manager, data)
   - Prioritizes `@timestamp` field (with fallback hierarchy)
   - Stores unmapped fields in `logData` JSON column
5. **Routes** documents to tables:
   - **Location-based**: If location matches `SPECIAL_LOCATIONS` → location table
   - **Agent-based**: Otherwise → agent name table
6. **Creates tables** dynamically with `ReplacingMergeTree()`
7. **Auto-expands schema**: Adds new columns as fields are discovered
8. **Batch inserts** data with configurable batch size
9. **Saves checkpoints** per table for resume capability
10. **Generates summary** with complete migration statistics

---

## Migration Summary Output

After migration, check `migration_summary.json`:

```json
{
  "total_processed": 150000,
  "total_inserted": 149500,
  "duplicates_skipped": 500,
  "errors": 0,
  "tables_created": ["default.wazuh_agent", "default.SOC1"],
  "tables_updated": {
    "default.wazuh_agent": 120000,
    "default.SOC1": 29500
  },
  "duration_seconds": 1800.5,
  "start_time": "2025-12-30T10:00:00Z",
  "end_time": "2025-12-30T10:30:00Z"
}
```

---

## Important Notes

### This is a ONE-TIME Migration Tool
- Use this ONLY for migrating historical data
- After migration, use `/root/clickhouse` for live ingestion
- Do not run continuously - it's not designed for real-time sync

### Duplicate Prevention
- Tracks OpenSearch document `_id` in `migrated_document_ids.json`
- Skips already migrated documents automatically during migration
- Uses same `MergeTree` engine as live ingestion for seamless integration
- Safe to re-run without data duplication

### Table Structure
- **Agent tables**: `default.wazuh_agent`, `default.agent_name`
- **Location tables**: `default.SOC1`, `default.India_DC` (from SPECIAL_LOCATIONS)
- **Schema**: `timestamp` (DateTime), `ingestion_date` (Date), flattened fields, `logData` (JSON)
- **Engine**: `MergeTree()` with daily partitioning by `ingestion_date`
- **Partitioning**: `PARTITION BY ingestion_date` (like Elasticsearch daily indices)
- **Order**: `ORDER BY (ingestion_date, timestamp)` for optimal query performance
- **Dynamic columns**: Auto-added as new fields discovered
- **Alignment**: Same schema as live ingestion in `/root/clickhouse`

### Performance Tips
- Default `BATCH_SIZE=1000` works for most cases
- Default `SCROLL_SIZE=5000` is optimal for OpenSearch queries
- **Don't set BATCH_SIZE > 10000** - use separate `SCROLL_SIZE` for query tuning
- Reduce batch size (500-250) for large documents or memory constraints
- Increase `SCROLL_SIZE` (up to 10000) for faster OpenSearch reads
- Use time ranges (`--from`, `--to`) to migrate in chunks for very large datasets
- Monitor `activity.log` for errors or performance issues
- Use `--dry-run` first to estimate migration time

**For Large Datasets (1TB+ or 20-30GB per index):**
- Use parallel workers: `--workers 4` (or 8, 16 depending on server resources)
- Parallel processing uses OpenSearch's sliced scroll feature (no 10K limit per slice)
- Each worker processes a different slice simultaneously, dramatically reducing migration time
- Recommended: 1 worker per 2-4 CPU cores available on ClickHouse server
- Example: `python opensearch_to_clickhouse.py --workers 8 --from "2024-01-01T00:00:00Z"`
- Monitor CPU and memory usage, adjust worker count accordingly

### OpenSearch Limitations
- **Max Result Window**: OpenSearch has a default `max_result_window` of 10,000 documents per scroll request
- **SCROLL_SIZE vs BATCH_SIZE**: 
  - `SCROLL_SIZE`: Controls how many documents are fetched per OpenSearch scroll (capped at 10,000)
  - `BATCH_SIZE`: Controls how many documents are batched before inserting into ClickHouse
  - These are now **separate parameters** for better performance tuning
- **Large Batch Sizes**: Setting `BATCH_SIZE` to very large values (e.g., 20,000) can:
  - Cause no data to be inserted if individual tables have fewer records than batch size
  - Create memory pressure on ClickHouse
  - Reduce migration progress visibility
- **Recommended**: Keep `BATCH_SIZE` between 1000-5000, use `SCROLL_SIZE` for query optimization
- **Sliced Scroll for Large Datasets**: When using `--workers > 1`, the tool uses OpenSearch's sliced scroll feature:
  - Bypasses the 10K scroll window limitation by splitting data into parallel slices
  - Each worker processes its own slice independently
  - Ideal for multi-TB datasets with billions of documents
  - Example: `--workers 8` splits data into 8 slices processed simultaneously

---

## Table Routing Logic

```python
# Documents route to tables based on:
if location in SPECIAL_LOCATIONS:
    table = f"{database}.{location}"  # e.g., default.SOC1
else:
    table = f"{database}.{agent_name}"  # e.g., default.wazuh_agent
```

**Example**:
- Document with `location: "SOC1"` → `default.SOC1` table
- Document with `agent.name: "wazuh_agent"` and `location: "Mumbai"` → `default.wazuh_agent` table

---

## Files Generated

- **`migration_summary.json`** - Complete migration statistics and results
- **`migration_checkpoints.json`** - Per-table progress checkpoints for resume
- **`migrated_document_ids.json`** - Tracked OpenSearch document IDs
- **`activity.log`** - Detailed operation logs with timestamps (includes scan diagnostics, document processing, and batch operations)
- **`.env`** - Configuration file (user-created)

> **Tip**: The `activity.log` now includes enhanced diagnostics showing:
> - OpenSearch scan initialization status
> - First few documents processed (for debugging)
> - Duplicate/checkpoint skip reasons
> - Batch creation and insertion progress
> - Progress updates every 1000 documents

---

## ClickHouse Table Schema

Tables are automatically created with:

```sql
CREATE TABLE {table_name} (
    timestamp DateTime,
    ingestion_date Date DEFAULT toDate(now()),
    -- Dynamically added columns based on document structure:
    agent_name Nullable(String),
    agent_id Nullable(String),
    rule_description Nullable(String),
    rule_level Nullable(Int64),
    data_integration Nullable(String),
    logData Nullable(String),  -- JSON for unmapped fields
    -- ... more columns added automatically ...
) ENGINE = MergeTree()
PARTITION BY ingestion_date
ORDER BY (ingestion_date, timestamp);
```

---

## Command Reference

```bash
# Test migration (no data written)
python opensearch_to_clickhouse.py --dry-run

# Full migration (all historical data)
python opensearch_to_clickhouse.py

# Time range migration
python opensearch_to_clickhouse.py \
  --from "2024-01-01T00:00:00Z" \
  --to "2024-12-31T23:59:59Z"

# Parallel processing for large datasets
python opensearch_to_clickhouse.py --workers 4

# Parallel with time range
python opensearch_to_clickhouse.py \
  --workers 8 \
  --from "2024-01-01T00:00:00Z" \
  --to "2024-12-31T23:59:59Z"

# Resume interrupted migration
python opensearch_to_clickhouse.py

# Reset and start fresh
python opensearch_to_clickhouse.py --reset-checkpoints
python opensearch_to_clickhouse.py

# Check migration logs
tail -f activity.log

# View migration results
cat migration_summary.json
```

---

## Verification Queries

After migration, verify in ClickHouse:

```sql
-- List all tables
SHOW TABLES;

-- Check record counts
SELECT COUNT(*) FROM default.wazuh_agent;
SELECT COUNT(*) FROM default.SOC1;

-- Check time range
SELECT 
  toDateTime(min(timestamp)) as earliest,
  toDateTime(max(timestamp)) as latest,
  count(*) as total
FROM default.wazuh_agent;

-- Sample data
SELECT * FROM default.wazuh_agent 
ORDER BY timestamp DESC 
LIMIT 10;

-- Check partitions by date
SELECT 
  partition,
  count() as records,
  formatReadableSize(sum(bytes)) as size
FROM system.parts
WHERE table = 'wazuh_agent' AND active
GROUP BY partition
ORDER BY partition DESC;

-- Drop old data by partition (much faster than DELETE)
ALTER TABLE default.wazuh_agent DROP PARTITION '2024-01-01';
```

---

## Use Cases

This tool is ideal for:

- **One-time bulk migration** from OpenSearch to ClickHouse
- **Historical data offloading** for long-term analytics and cost reduction
- **SIEM data migration** with agent and location segregation
- **Pre-live ingestion setup** before switching to real-time streaming
- **Compliance and audit** - migrate years of historical logs efficiently

---

## Documentation

- **[MIGRATION_WORKFLOW.md](MIGRATION_WORKFLOW.md)** - Detailed step-by-step migration guide, Phase 1 vs Phase 2 comparison, troubleshooting, and best practices
- **[LARGE_DATASET_GUIDE.md](LARGE_DATASET_GUIDE.md)** - Optimizing migration for 1TB+ datasets using parallel processing, chunking strategies, and performance tuning

---

## Conclusion

This is a **one-time bulk migration utility** designed to transfer historical data from OpenSearch to ClickHouse efficiently. After migration is complete:

1. Verify data in ClickHouse tables
2. Run OPTIMIZE on tables to merge duplicates
3. Switch to `/root/clickhouse` for live real-time ingestion

All migrated data can be queried via Grafana, Superset, or any BI tool connected to ClickHouse.

---

## Troubleshooting

### Migration Shows 0 Documents Migrated

If you see `0` documents migrated but OpenSearch has data:

1. **Check OpenSearch connectivity**: Verify credentials and index pattern in `.env`
2. **Check BATCH_SIZE**: If set too high (e.g., 20000), tables with fewer records won't trigger insertion
   - **Solution**: Set `BATCH_SIZE=1000` and use `SCROLL_SIZE` for performance tuning
3. **Check activity.log**: Look for:
   - "OpenSearch scan initialized successfully" (confirms query works)
   - Document processing logs (shows if documents are being read)
   - "checkpoint" skips (might be filtering out all documents)
4. **Check time range**: Documents might be outside your `--from`/`--to` range
5. **Check checkpoints**: Old checkpoints might skip all documents
   - **Solution**: `python opensearch_to_clickhouse.py --reset-checkpoints`
6. **Run dry-run with logging**: `python opensearch_to_clickhouse.py --dry-run` and check `activity.log`

### OpenSearch Query Fails

- **Error**: "Result window too large"
  - **Cause**: `SCROLL_SIZE` exceeds OpenSearch's `max_result_window`
  - **Solution**: Tool auto-caps at 10,000, but verify your OpenSearch settings
  
### ClickHouse Memory Errors

- **Error**: "Code: 241 - Memory limit exceeded"
  - **Cause**: `BATCH_SIZE` too large or documents too big
  - **Solution**: Reduce `BATCH_SIZE` to 500 or 250

### Migration Too Slow for Large Datasets (1TB+)

- **Problem**: Migration taking days/weeks for multi-TB datasets
- **Cause**: Sequential scroll processing is slow for billions of documents
- **Solution**: Use parallel workers with sliced scroll
  ```bash
  # Start with 4-8 workers
  python opensearch_to_clickhouse.py --workers 8
  
  # Monitor system resources and scale up
  htop  # Check CPU usage
  
  # Increase workers if CPU < 80%
  python opensearch_to_clickhouse.py --workers 16
  ```
- **Guidelines**:
  - 1-2 workers per CPU core available
  - Monitor ClickHouse server load
  - Test with `--dry-run --workers 4` first
  - Each worker maintains its own migrated IDs tracking
  
### OpenSearch Connection Timeout with Parallel Workers

- **Problem**: Connection errors when using many workers
- **Cause**: Too many concurrent connections to OpenSearch
- **Solution**: Balance workers with OpenSearch capacity
  ```bash
  # Reduce workers or increase OpenSearch connection pool
  python opensearch_to_clickhouse.py --workers 4  # Instead of 16
  ```

---

## Support

For detailed documentation, see [MIGRATION_WORKFLOW.md](MIGRATION_WORKFLOW.md).

For issues:
1. Check `activity.log` for detailed error messages
2. Review `migration_summary.json` for statistics
3. Verify `.env` configuration
4. Test connectivity to OpenSearch and ClickHouse separately

---
