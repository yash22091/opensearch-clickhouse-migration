# Large Dataset Migration Guide (1TB+)

## Overview

This guide is for migrating very large OpenSearch datasets (1TB+ or indices with 20-30GB+ each) to ClickHouse efficiently.

**Problem**: Sequential scrolling through billions of documents takes too long (days/weeks).

**Solution**: Parallel processing using OpenSearch's sliced scroll feature.

---

## Key Concepts

### Sliced Scroll

OpenSearch's sliced scroll allows splitting a dataset into multiple independent slices that can be processed in parallel:

- Each worker processes a different slice (portion) of data
- Workers run simultaneously, utilizing multiple CPU cores
- No overlap between slices - each document processed exactly once
- Bypasses the 10,000 scroll window limitation per worker

### Performance Gains

**Example**: 1TB dataset with 1 billion documents

| Workers | Est. Time | Speedup |
|---------|-----------|---------|
| 1       | 48 hours  | 1x      |
| 4       | 12 hours  | 4x      |
| 8       | 6 hours   | 8x      |
| 16      | 3 hours   | 16x     |

Actual performance depends on:
- Server CPU/RAM
- Network bandwidth to OpenSearch
- ClickHouse write performance
- Document complexity

---

## Migration Strategy for 1TB+ Datasets

### Step 1: Assessment

```bash
# Check total data size
curl -k -u "$OPENSEARCH_USER:$OPENSEARCH_PASS" \
  "https://$OPENSEARCH_HOST:$OPENSEARCH_PORT/_cat/indices/$OPENSEARCH_INDEX?v&h=index,store.size,docs.count"

# Expected output:
# index                      store.size  docs.count
# siem-alerts-2024-01  25.3gb      50000000
# siem-alerts-2024-02  27.1gb      55000000
# ...
```

**Determine**:
- Total documents to migrate
- Total data size
- Available server resources (CPU, RAM, network)

### Step 2: Resource Planning

**ClickHouse Server Requirements**:
- **CPU**: Minimum 8 cores, recommended 16-32 cores for parallel processing
- **RAM**: 32GB+ (more workers = more memory needed)
- **Disk**: SSD recommended, 2x source data size for safety
- **Network**: 1Gbps+ connection to OpenSearch

**Worker Count Guidelines**:
```
Recommended workers = (CPU cores / 2)

Examples:
- 8 CPU cores → 4 workers
- 16 CPU cores → 8 workers
- 32 CPU cores → 16 workers
- 64 CPU cores → 32 workers
```

### Step 3: Test Run

Always test with dry-run first:

```bash
# Test with 4 workers on a sample time range
python opensearch_to_clickhouse.py \
  --dry-run \
  --workers 4 \
  --from "2024-01-01T00:00:00Z" \
  --to "2024-01-07T23:59:59Z"

# Check logs
tail -f activity.log

# Look for:
# - All workers starting successfully
# - Documents being processed
# - No connection errors
# - Resource usage (htop, top)
```

### Step 4: Production Migration

#### Option A: Full Migration with Parallel Workers

For complete dataset migration:

```bash
# Start screen session
screen -S opensearch_migration

# Run with optimal worker count
python opensearch_to_clickhouse.py --workers 8

# Detach: Ctrl+A, D
```

#### Option B: Time-Range Chunking (Recommended for 1TB+)

Break migration into manageable chunks:

```bash
# Strategy: Migrate one month at a time with parallel workers

# Month 1: January 2024
screen -S migration_2024_01
python opensearch_to_clickhouse.py \
  --workers 8 \
  --from "2024-01-01T00:00:00Z" \
  --to "2024-01-31T23:59:59Z"
# Ctrl+A, D to detach

# Month 2: February 2024
screen -S migration_2024_02
python opensearch_to_clickhouse.py \
  --workers 8 \
  --from "2024-02-01T00:00:00Z" \
  --to "2024-02-29T23:59:59Z"
# Ctrl+A, D to detach

# Continue for remaining months...
```

**Advantages of chunking**:
- Can run multiple months in parallel on different servers
- Easier to restart if one chunk fails
- Better progress tracking
- Checkpoint management per chunk

#### Option C: Hybrid Approach (Best for Multi-TB)

Combine time-range chunking with parallel workers:

```bash
# Terminal 1: Process Q1 2024
screen -S migration_q1
python opensearch_to_clickhouse.py \
  --workers 8 \
  --from "2024-01-01T00:00:00Z" \
  --to "2024-03-31T23:59:59Z"

# Terminal 2: Process Q2 2024  
screen -S migration_q2
python opensearch_to_clickhouse.py \
  --workers 8 \
  --from "2024-04-01T00:00:00Z" \
  --to "2024-06-30T23:59:59Z"

# And so on...
```

### Step 5: Monitor Progress

```bash
# Monitor activity log
tail -f activity.log | grep "Worker"

# Expected output:
# 2026-03-19 10:15:23 INFO:Worker 0 (slice 0/8) starting
# 2026-03-19 10:15:23 INFO:Worker 1 (slice 1/8) starting
# ...
# 2026-03-19 10:20:45 INFO:Worker 0: 10000 processed, 9850 inserted
# 2026-03-19 10:20:46 INFO:Worker 3: 10000 processed, 9920 inserted

# Check system resources
htop

# Monitor ClickHouse server
watch -n 5 'clickhouse-client --query="SELECT COUNT(*) FROM default.wazuh_agent"'

# Check network throughput
iftop -i eth0
```

### Step 6: Verify and Optimize

After migration completes:

```sql
-- Connect to ClickHouse
clickhouse-client

-- Verify record counts
SELECT 
    table,
    sum(rows) as total_rows,
    formatReadableSize(sum(bytes)) as total_size
FROM system.parts
WHERE database = 'default' AND active
GROUP BY table
ORDER BY total_rows DESC;

-- Check time coverage
SELECT 
    toYYYYMMDD(min(timestamp)) as earliest_date,
    toYYYYMMDD(max(timestamp)) as latest_date,
    count(*) as total_records,
    count(DISTINCT toYYYYMMDD(timestamp)) as days_covered
FROM default.wazuh_agent;

-- Optimize tables (merge parts for better query performance)
OPTIMIZE TABLE default.wazuh_agent FINAL;
```

---

## Performance Tuning

### Adjusting Worker Count

Start conservatively and scale up:

```bash
# Day 1: Start with 4 workers
python opensearch_to_clickhouse.py --workers 4

# Monitor for 1-2 hours
# - Check CPU usage (should be 60-80%)
# - Check memory usage (< 80%)
# - Check no errors in logs

# Day 2: If resources allow, increase to 8
python opensearch_to_clickhouse.py --workers 8

# Continue scaling until optimal throughput
```

### Finding Optimal Settings

**CPU-bound**: CPU usage at 90%+
- **Good**: Workers working efficiently
- **Action**: Keep current worker count

**CPU-underutilized**: CPU usage < 50%
- **Issue**: Not enough parallelism or I/O bottleneck
- **Action**: Increase workers or check network/disk

**Memory issues**: RAM usage > 90%
- **Issue**: Too many workers or too large batch size
- **Action**: Reduce workers or reduce `BATCH_SIZE`

**Network bottleneck**: Network saturated but CPU < 50%
- **Issue**: Network bandwidth limiting throughput
- **Action**: Optimize network or reduce workers

### Configuration Tuning

```ini
# .env file for 1TB+ migration

# OpenSearch
OPENSEARCH_HOST=opensearch.example.com
OPENSEARCH_PORT=9200
OPENSEARCH_USER=admin
OPENSEARCH_PASS=password
OPENSEARCH_INDEX=siem-alerts-*

# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=password
CLICKHOUSE_DATABASE=default

# Performance tuning for large datasets
SPECIAL_LOCATIONS=SOC1,India-DC
BATCH_SIZE=2000          # Larger batches for efficiency
SCROLL_SIZE=10000        # Max out scroll size
PARALLEL_WORKERS=8       # Start with 8, adjust based on monitoring
```

---

## Troubleshooting Large Migrations

### Issue: Workers Failing with Memory Errors

**Symptoms**:
```
Worker 3: Error during processing: [Errno 12] Cannot allocate memory
```

**Solution**:
1. Reduce `BATCH_SIZE` from 2000 to 1000 or 500
2. Reduce number of workers
3. Increase server RAM
4. Enable swap (not recommended for production)

### Issue: OpenSearch Connection Timeouts

**Symptoms**:
```
Worker 2: Failed to initialize scan: Connection timeout
```

**Solution**:
1. Reduce worker count to decrease concurrent connections
2. Increase OpenSearch `http.max_content_length`
3. Check network stability
4. Use time-range chunking to reduce load

### Issue: Slow Progress Despite Low CPU

**Symptoms**:
- CPU usage: 30%
- Workers: 8
- Progress: Slow

**Possible Causes**:
1. **Network bottleneck**: Check with `iftop`
   - Solution: Ensure 1Gbps+ network
2. **Disk I/O bottleneck**: Check with `iostat`
   - Solution: Use SSD, optimize ClickHouse settings
3. **OpenSearch throttling**: Check OpenSearch logs
   - Solution: Increase OpenSearch resources
4. **ClickHouse write bottleneck**: Check ClickHouse logs
   - Solution: Optimize ClickHouse settings (increase buffer sizes)

### Issue: Data Duplication

**Symptoms**:
- Same documents appearing multiple times
- Record counts higher than expected

**Prevention**:
- Tool tracks document IDs automatically
- Each document processed only once
- Safe to restart/resume

**Verify**:
```sql
-- Check for duplicates (shouldn't find any with MergeTree)
SELECT _id, COUNT(*) as cnt
FROM default.wazuh_agent
GROUP BY _id
HAVING cnt > 1;
```

---

## Best Practices

### 1. Always Use Screen/Tmux

```bash
# Migration can take hours/days
screen -S opensearch_migration
python opensearch_to_clickhouse.py --workers 8
# Ctrl+A, D to detach

# Reattach anytime
screen -r opensearch_migration
```

### 2. Monitor Throughout Migration

Create a monitoring script:

```bash
#!/bin/bash
# monitor_migration.sh

while true; do
    clear
    echo "=== Migration Progress ==="
    echo
    
    echo "ClickHouse Records:"
    clickhouse-client --query="SELECT table, COUNT(*) FROM default.wazuh_agent" 2>/dev/null
    
    echo
    echo "Recent Activity Log:"
    tail -n 10 activity.log | grep "Worker"
    
    echo
    echo "System Resources:"
    top -b -n 1 | head -20
    
    sleep 30
done
```

### 3. Validate After Each Chunk

When using time-range chunking:

```bash
# After each month completes, verify
clickhouse-client --query="
SELECT 
    toYYYYMM(timestamp) as month,
    COUNT(*) as records
FROM default.wazuh_agent
WHERE toYYYYMM(timestamp) = 202401
GROUP BY month
"

# Compare with OpenSearch
curl -k -u "$OPENSEARCH_USER:$OPENSEARCH_PASS" \
  "https://$OPENSEARCH_HOST:$OPENSEARCH_PORT/siem-alerts-2024-01/_count"
```

### 4. Backup migration_summary.json

```bash
# After each successful migration chunk
cp migration_summary.json "migration_summary_$(date +%Y%m%d_%H%M%S).json"
```

### 5. Log Rotation

For long migrations, prevent log file from growing too large:

```bash
# Add to crontab
0 */6 * * * gzip /root/opensearch_to_clickhouse/activity.log && \
  mv /root/opensearch_to_clickhouse/activity.log.gz \
  "/root/opensearch_to_clickhouse/activity.log.$(date +%Y%m%d_%H%M%S).gz" && \
  touch /root/opensearch_to_clickhouse/activity.log
```

---

## Example: Migrating 1TB Dataset

**Scenario**:
- Total data: 1TB
- Documents: 1 billion
- Indices: 12 months, ~30GB each
- ClickHouse server: 32 CPU cores, 64GB RAM
- Goal: Complete in 24 hours

**Strategy**:

```bash
# Use 16 workers for maximum parallelism
# Migrate 3 months in parallel (4 sessions)

# Session 1: Q1 2024
screen -S mig_q1
python opensearch_to_clickhouse.py --workers 16 \
  --from "2024-01-01T00:00:00Z" --to "2024-03-31T23:59:59Z"

# Session 2: Q2 2024
screen -S mig_q2
python opensearch_to_clickhouse.py --workers 16 \
  --from "2024-04-01T00:00:00Z" --to "2024-06-30T23:59:59Z"

# Session 3: Q3 2024
screen -S mig_q3
python opensearch_to_clickhouse.py --workers 16 \
  --from "2024-07-01T00:00:00Z" --to "2024-09-30T23:59:59Z"

# Session 4: Q4 2024
screen -S mig_q4
python opensearch_to_clickhouse.py --workers 16 \
  --from "2024-10-01T00:00:00Z" --to "2024-12-31T23:59:59Z"
```

**Expected Timeline**:
- Each quarter: ~250GB, ~250M docs
- With 16 workers: ~6 hours per quarter
- Running 4 in parallel: Complete in 6 hours total
- Total time including validation: ~8 hours

**Resource Usage**:
- CPU: 90-95% (optimal)
- RAM: ~48GB (75% of available)
- Network: ~400Mbps sustained
- Disk I/O: ~200MB/s write

---

## Summary

For large datasets (1TB+):

1. **Use parallel workers**: `--workers 8` (or more based on CPU)
2. **Chunk by time range**: Migrate months/quarters separately
3. **Monitor continuously**: CPU, RAM, network, progress logs
4. **Validate incrementally**: Verify each chunk before proceeding
5. **Use screen/tmux**: Essential for long-running migrations
6. **Scale progressively**: Start small, increase workers as comfortable

With proper parallel processing, migrating multi-TB datasets becomes feasible and can complete in hours instead of days or weeks.

---
