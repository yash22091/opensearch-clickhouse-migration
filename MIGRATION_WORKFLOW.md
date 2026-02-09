# Complete Migration and Ingestion Workflow

## 📋 Overview

This document explains the two-phase approach for moving from OpenSearch to ClickHouse:

```
┌─────────────────────────────────────────────────────────────────┐
│                         PHASE 1: MIGRATION                       │
│              (Use /root/opensearch_to_clickhouse)                │
│                                                                   │
│  OpenSearch              One-Time Migration              ClickHouse │
│  (Historical) ───────────────────────────────────────► (Tables)  │
│  Data                                                              │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    PHASE 2: LIVE INGESTION                       │
│                   (Use /root/clickhouse)                          │
│                                                                   │
│  New Data            Real-Time Ingestion              ClickHouse │
│  Source  ────────────────────────────────────────────► (Tables)  │
│  (Kafka/Stream)                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Phase 1: Historical Data Migration

### Tool: `/root/opensearch_to_clickhouse`

**Purpose**: Migrate ALL historical data from OpenSearch to ClickHouse as a one-time operation.

### Key Features:
- ✅ **Duplicate Prevention**: Tracks document IDs, won't re-import same data
- ✅ **Resume Capability**: Checkpoints allow resuming interrupted migrations
- ✅ **Progress Tracking**: Real-time progress and detailed summary reports
- ✅ **Dry Run Mode**: Test migration without writing data
- ✅ **Daily Partitioning**: Tables partitioned by `ingestion_date` (like Elasticsearch daily indices)
- ✅ **Table Auto-Creation**: Creates tables with `MergeTree` engine aligned with live ingestion
- ✅ **Schema Flexibility**: Auto-adds columns as new fields are discovered
- ✅ **Seamless Integration**: Same schema as `/root/clickhouse` for unified data access

### When to Use:
- ✅ Initial setup when switching from OpenSearch to ClickHouse
- ✅ Migrating months/years of historical logs
- ✅ One-time bulk data transfer
- ❌ NOT for continuous syncing
- ❌ NOT for real-time data ingestion

### Migration Process:

#### Step 1: Prepare Environment
```bash
cd /root/opensearch_to_clickhouse
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

#### Step 2: Configure .env
```bash
# OpenSearch Configuration
OPENSEARCH_HOST=your-opensearch-host
OPENSEARCH_PORT=9200
OPENSEARCH_USER=admin
OPENSEARCH_PASS=your-password
OPENSEARCH_INDEX=invinsense-alerts-*

# ClickHouse Configuration
CLICKHOUSE_HOST=your-clickhouse-host
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=default

# Migration Settings
BATCH_SIZE=1000
SPECIAL_LOCATIONS=SOC1,India-DC
```

#### Step 3: Test Migration (Dry Run)
```bash
# Test without writing data
python opensearch_to_clickhouse.py --dry-run

# Test specific time range
python opensearch_to_clickhouse.py --dry-run \
  --from "2024-01-01T00:00:00Z" \
  --to "2024-12-31T23:59:59Z"
```

#### Step 4: Execute Migration
```bash
# Migrate all historical data
python opensearch_to_clickhouse.py

# Or migrate by time ranges (for very large datasets)
python opensearch_to_clickhouse.py --from "2024-01-01T00:00:00Z" --to "2024-06-30T23:59:59Z"
python opensearch_to_clickhouse.py --from "2024-07-01T00:00:00Z" --to "2024-12-31T23:59:59Z"
```

#### Step 5: Monitor Progress
```bash
# Watch real-time progress in terminal
# Check activity log
tail -f activity.log

# View migration summary after completion
cat migration_summary.json
```

#### Step 6: Verify Migration
```bash
# Check ClickHouse tables
clickhouse-client --query "SHOW TABLES"

# Verify record counts
clickhouse-client --query "SELECT COUNT(*) FROM default.wazuh_agent"

# Check sample data
clickhouse-client --query "SELECT * FROM default.wazuh_agent LIMIT 10"
```

### Generated Files:
- `migration_summary.json` - Complete migration statistics
- `migration_checkpoints.json` - Resume points for each table
- `migrated_document_ids.json` - List of migrated OpenSearch document IDs
- `activity.log` - Detailed operation logs

### Expected Output:
```
================================================================================
OpenSearch to ClickHouse Migration - LIVE MODE
Started at: 2025-12-30T10:00:00Z
================================================================================

Querying OpenSearch index: invinsense-alerts-*
Time range: {'gt': '1970-01-01T00:00:00Z'}

Progress: 10000 documents processed, 9950 inserted, 50 duplicates skipped
Progress: 20000 documents processed, 19900 inserted, 100 duplicates skipped
...

================================================================================
Migration Summary - COMPLETED
================================================================================
Duration: 1800.50 seconds
Total documents processed: 150000
Documents inserted: 149500
Duplicates skipped: 500
Errors: 0
Tables created: 2

Records per table:
  - default.wazuh_agent: 120000 records
  - default.SOC1: 29500 records

Migration summary saved to: migration_summary.json
================================================================================
```

---

## ⚡ Phase 2: Live Data Ingestion

### Tool: `/root/clickhouse`

**Purpose**: Continuous real-time ingestion of NEW data into ClickHouse.

### When to Use:
- ✅ After Phase 1 migration is complete
- ✅ For real-time/near-real-time data ingestion
- ✅ Continuous operation (runs as a service/daemon)
- ✅ Handles streaming data from Kafka, APIs, etc.

### Key Differences from Migration Tool:

| Feature | Phase 1 (Migration) | Phase 2 (Live Ingestion) |
|---------|---------------------|--------------------------|
| **Purpose** | One-time bulk transfer | Continuous real-time ingestion |
| **Source** | OpenSearch historical data | Kafka/Stream/API |
| **Operation** | Run once and complete | Runs continuously |
| **Duplicate Handling** | Tracks document IDs | Depends on implementation |
| **Checkpointing** | File-based checkpoints | Kafka offsets / timestamps |
| **Performance** | Batch-oriented | Stream-oriented |
| **Resumability** | Yes, from checkpoint | Yes, from offset/timestamp |

### Transition Checklist:

Before switching to live ingestion:

- [ ] Phase 1 migration completed successfully
- [ ] Verified all data in ClickHouse tables
- [ ] Reviewed `migration_summary.json` - no major errors
- [ ] Tested sample queries on migrated data
- [ ] Configured `/root/clickhouse` for your data source
- [ ] Set up monitoring for live ingestion
- [ ] Documented cutover timestamp (when live ingestion starts)

### Cutover Strategy:

```
Timeline:
─────────────────────────────────────────────────────────────────
        Historical Data              |      New Data
    (OpenSearch Archive)             |  (Live Stream)
─────────────────────────────────────|────────────────────────────
                                     ↑
                              Cutover Point
                           (e.g., 2025-12-30T00:00:00Z)

Phase 1: Migrate everything BEFORE cutover point
Phase 2: Ingest everything FROM cutover point onwards
```

**Recommendation**: Have a small overlap (5-10 minutes) to ensure no data loss:
- Migrate until: `2025-12-30T00:05:00Z`
- Start live ingestion from: `2025-12-30T00:00:00Z`
- ReplacingMergeTree will handle the duplicates automatically

---

## 🔍 Troubleshooting

### Migration Issues:

**Problem**: Migration interrupted
```bash
# Solution: Simply re-run, it will resume from checkpoint
python opensearch_to_clickhouse.py
```

**Problem**: Want to restart fresh
```bash
# Solution: Reset checkpoints
python opensearch_to_clickhouse.py --reset-checkpoints
# Then run migration again
python opensearch_to_clickhouse.py
```

**Problem**: Duplicate data after re-running
```bash
# Solution: The tool tracks document IDs in migrated_document_ids.json
# Duplicates are automatically skipped during migration
# Migration and live ingestion use the same MergeTree schema for seamless integration
```

**Problem**: Out of memory errors
```bash
# Solution: Reduce batch size in .env
BATCH_SIZE=500  # or even 250 for large documents
```

**Problem**: Need to verify specific time range migrated
```bash
# Check ClickHouse data range
clickhouse-client --query "
  SELECT 
    min(timestamp) as earliest,
    max(timestamp) as latest,
    count(*) as total
  FROM default.wazuh_agent
"
```

---

## 📊 Best Practices

### 1. Test Before Full Migration
Always run with `--dry-run` first to identify issues

### 2. Migrate in Time Chunks (for very large datasets)
```bash
# Year by year for multi-year data
python opensearch_to_clickhouse.py --from "2023-01-01T00:00:00Z" --to "2023-12-31T23:59:59Z"
python opensearch_to_clickhouse.py --from "2024-01-01T00:00:00Z" --to "2024-12-31T23:59:59Z"
```

### 3. Monitor ClickHouse Disk Space
```bash
# Check disk usage
clickhouse-client --query "
  SELECT 
    table,
    formatReadableSize(sum(bytes)) as size
  FROM system.parts
  WHERE active
  GROUP BY table
"
```

### 4. Optimize Tables After Migration
```bash
# Merge duplicates and optimize storage
clickhouse-client --query "OPTIMIZE TABLE default.wazuh_agent FINAL"
```

### 5. Backup Before Major Operations
```bash
# Backup checkpoint files before resetting
cp migration_checkpoints.json migration_checkpoints.json.backup
cp migrated_document_ids.json migrated_document_ids.json.backup
```

---

## 🎯 Quick Reference

### Migration Commands
```bash
# Dry run test
python opensearch_to_clickhouse.py --dry-run

# Full migration
python opensearch_to_clickhouse.py

# Time range migration
python opensearch_to_clickhouse.py --from "2024-01-01T00:00:00Z" --to "2024-12-31T23:59:59Z"

# Resume interrupted migration
python opensearch_to_clickhouse.py

# Reset and start fresh
python opensearch_to_clickhouse.py --reset-checkpoints
```

### Verification Queries
```sql
-- Check all tables
SHOW TABLES;

-- Check record counts
SELECT COUNT(*) FROM default.wazuh_agent;

-- Check time range
SELECT 
  toDateTime(min(timestamp)) as earliest,
  toDateTime(max(timestamp)) as latest,
  count(*) as total
FROM default.wazuh_agent;

-- Check partitions
SELECT 
  partition,
  count() as records,
  formatReadableSize(sum(bytes)) as size
FROM system.parts
WHERE table = 'wazuh_agent' AND active
GROUP BY partition
ORDER BY partition DESC;

-- Drop old partitions (if needed)
ALTER TABLE default.wazuh_agent DROP PARTITION '2024-01-01';
```

---

## 📞 Support

If issues persist:
1. Check `activity.log` for detailed errors
2. Review `migration_summary.json` for statistics
3. Verify `.env` configuration
4. Test ClickHouse and OpenSearch connectivity separately
5. Check network/firewall settings

---

## 📝 Summary

- **Phase 1** (This tool): One-time migration of historical data from OpenSearch
- **Phase 2** (`/root/clickhouse`): Continuous live data ingestion
- **Sequence**: Complete Phase 1 fully before starting Phase 2
- **Safety**: Tool prevents duplicates, safe to re-run
- **Monitoring**: Use summary files and logs to track progress
