import json
import argparse
import logging
import os
import re
import time
import asyncio
from datetime import datetime, timezone

from dotenv import load_dotenv
from clickhouse_driver import Client
from opensearchpy import OpenSearch
from opensearchpy.helpers import scan
from opensearchpy.helpers.errors import ScanError
import dateutil.parser as dateparser
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

# Environment Variables
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')

OPENSEARCH_HOST = os.getenv('OPENSEARCH_HOST')
OPENSEARCH_PORT = int(os.getenv('OPENSEARCH_PORT', 9200))
OPENSEARCH_USER = os.getenv('OPENSEARCH_USER')
OPENSEARCH_PASS = os.getenv('OPENSEARCH_PASS')
OPENSEARCH_INDEX = os.getenv('OPENSEARCH_INDEX', 'invinsense-alerts-*')

SPECIAL_LOCATIONS = [loc.strip() for loc in os.getenv('SPECIAL_LOCATIONS', '').split(',') if loc.strip()]
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
# OpenSearch has a max result window, cap scroll size at 10000
SCROLL_SIZE = min(int(os.getenv('SCROLL_SIZE', 5000)), 10000)
CHECKPOINT_FILE = "migration_checkpoints.json"
MIGRATED_IDS_FILE = "migrated_document_ids.json"
MIGRATION_SUMMARY_FILE = "migration_summary.json"

logging.basicConfig(
    filename='activity.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

def connect_clickhouse():
    retries = 5
    while retries > 0:
        try:
            client = Client(
                host=CLICKHOUSE_HOST,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD
            )
            logging.info("Connected to ClickHouse")
            return client
        except Exception as e:
            logging.error(f"Failed to connect to ClickHouse: {e}")
            retries -= 1
            time.sleep(5)
    raise RuntimeError("Unable to connect to ClickHouse")

def sanitize_column_name(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

def sanitize_table_name(name):
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    return name if name[0].isalpha() else f"table_{name}"[:63]

def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def get_clickhouse_type(value):
    if isinstance(value, bool): return 'Nullable(UInt8)'
    elif isinstance(value, int): return 'Nullable(Int64)'
    elif isinstance(value, float): return 'Nullable(Float64)'
    return 'Nullable(String)'

def create_table_if_not_exist(client, table_name, columns):
    if not client.execute(f"EXISTS TABLE {table_name}")[0][0]:
        column_defs = ', '.join([f"`{sanitize_column_name(col)}` {get_clickhouse_type(val)}" for col, val in columns.items() if col not in ['timestamp']])
        query = f"""
            CREATE TABLE {table_name} (
                timestamp DateTime,
                {column_defs}
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMMDD(timestamp)
            ORDER BY timestamp
        """
        client.execute(query)
        logging.info(f"Created table {table_name} with MergeTree engine, partitioned by timestamp date")

def get_table_name(agent_name, location):
    return f"{CLICKHOUSE_DATABASE}.{sanitize_table_name(location if location in SPECIAL_LOCATIONS else agent_name)}"

def convert_to_type(value, expected_type):
    try:
        if expected_type == 'Nullable(Int64)': return int(value)
        elif expected_type == 'Nullable(Float64)': return float(value)
        elif expected_type == 'Nullable(UInt8)': return int(bool(value))
        else: return str(value) if value is not None else None
    except: return None

def process_document(source, doc_id):
    try:
        agent_name = source.get('agent', {}).get('name', 'default_agent')
        location = source.get('location') or source.get('data', {}).get('integration')
        flattened_data = {}
        for key in ['manager', 'rule', 'agent', 'data']:
            flattened_data.update(flatten_dict(source.get(key, {}), parent_key=key))
        # Prioritize @timestamp as primary field for consistency
        timestamp_str = (
            source.get('@timestamp') or
            source.get('timestamp') or
            source.get('vulnerability', {}).get('detected_at') or
            source.get('event', {}).get('created') or
            None
        )
        dt = dateparser.parse(timestamp_str) if timestamp_str else datetime.now(timezone.utc)
        insert_data = {'timestamp': dt, **flattened_data}
        other_keys = set(source) - {'manager', 'rule', 'agent', 'data', '@timestamp', 'timestamp'}
        insert_data['logData'] = json.dumps({k: source[k] for k in other_keys if source[k] is not None})
        return get_table_name(agent_name, location), insert_data, dt.isoformat() + 'Z'
    except Exception as e:
        logging.error(f"Error processing document: {e}")
        return None, None, None

async def bulk_insert(client, table_name, data_batch):
    try:
        if not data_batch: return
        existing_columns = {row[0]: row[1] for row in client.execute(f"DESCRIBE {table_name}")}
        all_keys = set(k for row in data_batch for k in row.keys())
        sanitized_keys = {k: sanitize_column_name(k) for k in all_keys}
        for orig, sani in sanitized_keys.items():
            if sani not in existing_columns and sani not in ['timestamp']:
                sample_val = next((row[orig] for row in data_batch if orig in row), None)
                if sample_val is not None:
                    col_type = get_clickhouse_type(sample_val)
                    client.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS `{sani}` {col_type}")
                    existing_columns[sani] = col_type
        cols = ['timestamp'] + [sanitized_keys[k] for k in all_keys if k not in ['timestamp']]
        values = [[row.get('timestamp')] + [convert_to_type(row.get(k), existing_columns[sanitized_keys[k]]) for k in all_keys if k not in ['timestamp']] for row in data_batch]
        client.execute(f"INSERT INTO {table_name} ({', '.join(f'`{c}`' for c in cols)}) VALUES", values)
        logging.info(f"Inserted {len(values)} rows into {table_name}")
    except Exception as e:
        if "Code: 241" in str(e) and len(data_batch) > 10:
            logging.warning(f"Memory error on {table_name}, splitting batch")
            mid = len(data_batch) // 2
            await bulk_insert(client, table_name, data_batch[:mid])
            await bulk_insert(client, table_name, data_batch[mid:])
        else:
            logging.error(f"Failed insert into {table_name}: {e}")
            raise

async def list_indices(os_client):
    try:
        indices = os_client.cat.indices(format="json")
        logging.info("Available OpenSearch indices:")
        for idx in indices:
            logging.info(f" - {idx['index']}")
    except Exception as e:
        logging.warning(f"Index discovery failed: {e}")

def load_checkpoints():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE) as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"Failed to load checkpoints: {e}")
    return {}

def load_migrated_ids():
    if os.path.exists(MIGRATED_IDS_FILE):
        try:
            with open(MIGRATED_IDS_FILE) as f:
                return set(json.load(f))
        except Exception as e:
            logging.warning(f"Failed to load migrated IDs: {e}")
    return set()

def save_migrated_ids(migrated_ids):
    try:
        with open(MIGRATED_IDS_FILE, 'w') as f:
            json.dump(list(migrated_ids), f)
    except Exception as e:
        logging.error(f"Could not save migrated IDs: {e}")

def save_checkpoint(table_name, last_ts):
    cp = load_checkpoints()
    cp[table_name] = last_ts
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(cp, f, indent=2)
    except Exception as e:
        logging.error(f"Could not save checkpoint: {e}")

def reset_checkpoints():
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        logging.info("Checkpoints reset")
    if os.path.exists(MIGRATED_IDS_FILE):
        os.remove(MIGRATED_IDS_FILE)
        logging.info("Migrated IDs reset")
    if os.path.exists(MIGRATION_SUMMARY_FILE):
        os.remove(MIGRATION_SUMMARY_FILE)
        logging.info("Migration summary reset")

async def migrate(from_ts=None, to_ts=None, dry_run=False):
    start_time = datetime.now(timezone.utc)
    print(f"\n{'='*80}")
    print(f"OpenSearch to ClickHouse Migration - {'DRY RUN' if dry_run else 'LIVE MODE'}")
    print(f"Started at: {start_time.isoformat()}")
    print(f"{'='*80}\n")
    
    client = connect_clickhouse()
    os_client = OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
        use_ssl=True,
        verify_certs=False
    )

    await list_indices(os_client)
    checkpoints = load_checkpoints()
    migrated_ids = load_migrated_ids()
    
    # Migration statistics
    stats = {
        'total_processed': 0,
        'total_inserted': 0,
        'duplicates_skipped': 0,
        'errors': 0,
        'tables_created': [],
        'tables_updated': {},
        'start_time': start_time.isoformat(),
        'time_range': {'from': from_ts or '1970-01-01T00:00:00Z', 'to': to_ts or 'NOW'}
    }

    range_query = {"gt": from_ts or "1970-01-01T00:00:00Z"}
    if to_ts:
        range_query["lt"] = to_ts

    # Use must with @timestamp as primary field
    query = {
        "query": {
            "range": {"@timestamp": range_query}
        }
    }
    
    print(f"Querying OpenSearch index: {OPENSEARCH_INDEX}")
    print(f"Time range: {range_query}")
    print(f"Scroll size: {SCROLL_SIZE}, Batch insert size: {BATCH_SIZE}\n")
    
    logging.info(f"Starting migration - Index: {OPENSEARCH_INDEX}, Time range: {range_query}")
    
    try:
        results = scan(
            client=os_client,
            index=OPENSEARCH_INDEX,
            query=query,
            scroll="5m",
            size=SCROLL_SIZE,  # Use SCROLL_SIZE instead of BATCH_SIZE
            preserve_order=True,
            raise_on_error=False
        )
        logging.info("OpenSearch scan initialized successfully")
    except ScanError as e:
        logging.error(f"Partial scan failure: {e}")
        stats['errors'] += 1
        return stats
    except Exception as e:
        logging.error(f"Failed to initialize OpenSearch scan: {e}")
        stats['errors'] += 1
        return stats

    batches = {}
    max_timestamp_tracker = {}
    new_migrated_ids = set()
    doc_count = 0

    logging.info("Starting document processing loop")
    print("Processing documents...\n")
    
    try:
        for item in results:
            doc_count += 1
            stats['total_processed'] += 1
        doc_id = item['_id']
        source = item['_source']
        
        # Log first few documents for debugging
        if doc_count <= 5:
            logging.info(f"Processing document {doc_count}: ID={doc_id}")
        
        # Skip if already migrated
        if doc_id in migrated_ids:
            stats['duplicates_skipped'] += 1
            if doc_count <= 10:
                logging.info(f"Document {doc_id} already migrated, skipping")
            continue
        
        table_name, insert_data, doc_ts = process_document(source, doc_id)
        if not table_name or not insert_data or not doc_ts:
            stats['errors'] += 1
            logging.warning(f"Failed to process document {doc_id}")
            continue

        last_cp_ts = checkpoints.get(table_name, "1970-01-01T00:00:00Z")
        if doc_ts <= last_cp_ts:
            if doc_count <= 10:
                logging.info(f"Document {doc_id} timestamp {doc_ts} <= checkpoint {last_cp_ts}, skipping")
            continue

        batches.setdefault(table_name, []).append(insert_data)
        new_migrated_ids.add(doc_id)
        ts = insert_data['timestamp']
        
        # Log batch progress
        if len(batches[table_name]) == 1:
            logging.info(f"Started new batch for table {table_name}")
        
        if table_name not in max_timestamp_tracker or ts > max_timestamp_tracker[table_name]:
            max_timestamp_tracker[table_name] = ts
            
        if len(batches[table_name]) >= BATCH_SIZE:
            if not dry_run:
                if table_name not in stats['tables_created']:
                    create_table_if_not_exist(client, table_name, insert_data)
                    stats['tables_created'].append(table_name)
                await bulk_insert(client, table_name, batches[table_name])
                save_checkpoint(table_name, max_timestamp_tracker[table_name].isoformat() + 'Z')
                stats['tables_updated'][table_name] = stats['tables_updated'].get(table_name, 0) + len(batches[table_name])
                stats['total_inserted'] += len(batches[table_name])
            batches[table_name] = []
            
        # Progress reporting every 1000 documents
        if stats['total_processed'] % 1000 == 0:
            print(f"Progress: {stats['total_processed']} documents processed, {stats['total_inserted']} inserted, {stats['duplicates_skipped']} duplicates skipped")
            logging.info(f"Progress: {stats['total_processed']} processed, {len(batches)} tables with pending batches")

    except Exception as e:
        logging.error(f"Error during document processing: {e}")
        print(f"Error processing documents: {e}")
        stats['errors'] += 1

    # Log completion of scan
    logging.info(f"Document scan completed: {doc_count} total documents retrieved from OpenSearch")
    print(f"\nDocument scan completed: {doc_count} documents retrieved from OpenSearch\n")
    
    # Insert remaining batches
    print(f"\nInserting remaining batches for {len(batches)} tables...")
    logging.info(f"Processing remaining batches: {len(batches)} tables with pending data")
    
    for table_name, batch in batches.items():
        if batch:
            logging.info(f"Inserting remaining {len(batch)} records into {table_name}")
            if not dry_run:
                if table_name not in stats['tables_created']:
                    create_table_if_not_exist(client, table_name, batch[0])
                    stats['tables_created'].append(table_name)
                await bulk_insert(client, table_name, batch)
                save_checkpoint(table_name, max_timestamp_tracker[table_name].isoformat() + 'Z')
                stats['tables_updated'][table_name] = stats['tables_updated'].get(table_name, 0) + len(batch)
                stats['total_inserted'] += len(batch)
            else:
                logging.info(f"DRY RUN: Would insert {len(batch)} records into {table_name}")

    # Save migrated IDs
    if not dry_run:
        migrated_ids.update(new_migrated_ids)
        save_migrated_ids(migrated_ids)
    
    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    stats['end_time'] = end_time.isoformat()
    stats['duration_seconds'] = duration
    
    # Save summary
    if not dry_run:
        with open(MIGRATION_SUMMARY_FILE, 'w') as f:
            json.dump(stats, f, indent=2)
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"Migration Summary - {'DRY RUN' if dry_run else 'COMPLETED'}")
    print(f"{'='*80}")
    print(f"Duration: {duration:.2f} seconds")
    print(f"Total documents processed: {stats['total_processed']}")
    print(f"Documents inserted: {stats['total_inserted']}")
    print(f"Duplicates skipped: {stats['duplicates_skipped']}")
    print(f"Errors: {stats['errors']}")
    print(f"Tables created: {len(stats['tables_created'])}")
    print(f"\nRecords per table:")
    for table, count in stats['tables_updated'].items():
        print(f"  - {table}: {count} records")
    print(f"\nMigration summary saved to: {MIGRATION_SUMMARY_FILE}")
    print(f"{'='*80}\n")
    
    logging.info(f"Migration completed: {stats['total_inserted']} documents inserted in {duration:.2f}s")
    return stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate historical data from OpenSearch to ClickHouse (one-time migration tool)"
    )
    parser.add_argument("--reset-checkpoints", action="store_true", 
                        help="Reset all checkpoints and migrated document IDs")
    parser.add_argument("--from", dest="from_ts", 
                        help="Start timestamp (ISO format: 2024-01-01T00:00:00Z)")
    parser.add_argument("--to", dest="to_ts", 
                        help="End timestamp (ISO format: 2024-12-31T23:59:59Z)")
    parser.add_argument("--dry-run", action="store_true", 
                        help="Test migration without inserting data")
    args = parser.parse_args()

    if args.reset_checkpoints:
        reset_checkpoints()
        print("All migration checkpoints and tracked IDs have been reset.")
    else:
        asyncio.run(migrate(from_ts=args.from_ts, to_ts=args.to_ts, dry_run=args.dry_run))