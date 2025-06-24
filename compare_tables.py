import json
import snowflake.connector
from typing import List
from datetime import datetime

# CONFIG
CONFIG_PATH = 'config.json'
TABLE_1 = "FIVETRAN_RAW.SCOTTS.SCOTTS"
TABLE_2 = "_CLONE_DATAOPS_PENG_8343_SNOWPIPE_RAW.SCOTTS_PIPE.SCOTTS"
EXCLUDED_COLUMNS = ['METADATA_FILENAME', 'METADATA_FILE_ROW_NUMBER', '_DBT_COPIED_AT', '_FILE', '_FIVETRAN_SYNCED', '_LINE', '_MODIFIED']

# Generate log file path with timestamp
def create_log_file():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"compare_log_{timestamp}.txt"

# Write both to console and log file
class Logger:
    def __init__(self, filepath):
        self.file = open(filepath, "w", encoding="utf-8")

    def log(self, message):
        print(message)
        self.file.write(message + "\n")

    def close(self):
        self.file.close()

# Load config
def load_config(path=CONFIG_PATH):
    with open(path, 'r') as f:
        return json.load(f)

# Connect to Snowflake via browser-based SSO
def connect_to_snowflake(config):
    return snowflake.connector.connect(
        user=config["user"],
        account=config["account"],
        warehouse=config["warehouse"],
        authenticator="externalbrowser"
    )

# Get columns (excluding specified)
def get_columns(cursor, table_name: str, excluded: List[str]) -> List[str]:
    cursor.execute(f"DESC TABLE {table_name}")
    return [row[0] for row in cursor.fetchall() if row[0] not in excluded]

# Get count of distinct values for each column
def get_distinct_counts(cursor, table_name: str, columns: List[str]) -> dict:
    counts = {}
    for col in columns:
        query = f'SELECT COUNT(DISTINCT "{col}") FROM {table_name}'
        cursor.execute(query)
        counts[col] = cursor.fetchone()[0]
    return counts

# Get count of records for each distinct value in a column
def get_value_counts(cursor, table_name: str, column: str) -> dict:
    query = f'SELECT "{column}", COUNT(*) FROM {table_name} GROUP BY "{column}"'
    cursor.execute(query)
    return {row[0]: row[1] for row in cursor.fetchall()}

# Main comparison logic
def compare_tables():
    config = load_config()
    log_path = create_log_file()
    logger = Logger(log_path)

    with connect_to_snowflake(config) as conn:
        with conn.cursor() as cursor:

            logger.log(f"\n🔍 Comparing tables:")
            logger.log(f"    • Table 1: {TABLE_1}")
            logger.log(f"    • Table 2: {TABLE_2}\n")

            # Row count check
            cursor.execute(f"SELECT COUNT(*) FROM {TABLE_1}")
            row_count_1 = cursor.fetchone()[0]
            cursor.execute(f"SELECT COUNT(*) FROM {TABLE_2}")
            row_count_2 = cursor.fetchone()[0]
            logger.log(f"🔢 Row count in {TABLE_1}: {row_count_1}")
            logger.log(f"🔢 Row count in {TABLE_2}: {row_count_2}")
            if row_count_1 != row_count_2:
                logger.log("❌ Tables do NOT have the same number of rows.\n")
            else:
                logger.log("✅ Both tables have the same number of rows.\n")

            # Column names comparison across two tables
            cols_1 = get_columns(cursor, TABLE_1, EXCLUDED_COLUMNS)
            cols_2 = get_columns(cursor, TABLE_2, EXCLUDED_COLUMNS)

            set_1 = set(cols_1)
            set_2 = set(cols_2)

            only_in_1 = sorted(set_1 - set_2)
            only_in_2 = sorted(set_2 - set_1)
            common = sorted(set_1 & set_2)

            logger.log(f"📌 Columns only in {TABLE_1}: {only_in_1 or 'None'}")
            logger.log(f"📌 Columns only in {TABLE_2}: {only_in_2 or 'None'}\n")

            # Distinct value comparison across two tables
            logger.log(f"📊 Distinct value comparison for {len(common)} common columns:\n")
            counts_1 = get_distinct_counts(cursor, TABLE_1, common)
            counts_2 = get_distinct_counts(cursor, TABLE_2, common)

            mismatches = []
            for col in common:
                if counts_1[col] != counts_2[col]:
                    mismatches.append((col, counts_1[col], counts_2[col]))

            if mismatches:
                for col, c1, c2 in mismatches:
                    logger.log(f"⚠️  Column '{col}': {TABLE_1} has {c1}, {TABLE_2} has {c2}")
                num_ok = len(common) - len(mismatches)
                logger.log(f"\n✅ Remaining {num_ok} columns have matching distinct counts.\n")
            else:
                logger.log("\n✅ All common columns have matching distinct counts.\n")

            # Value-level comparison for all common columns
            logger.log("\n🔬 Value-level comparison for all common columns:")
            for col in common:
                value_counts_1 = get_value_counts(cursor, TABLE_1, col)
                value_counts_2 = get_value_counts(cursor, TABLE_2, col)

                values_1 = set(value_counts_1.keys())
                values_2 = set(value_counts_2.keys())

                missing_in_2 = values_1 - values_2
                missing_in_1 = values_2 - values_1

                if missing_in_2:
                    logger.log(f"⚠️  Values in {TABLE_1}.{col} but not in {TABLE_2}: {sorted(missing_in_2)}")
                if missing_in_1:
                    logger.log(f"⚠️  Values in {TABLE_2}.{col} but not in {TABLE_1}: {sorted(missing_in_1)}")

                # Compare counts for shared values
                shared = values_1 & values_2
                count_mismatches = []
                for v in shared:
                    if value_counts_1[v] != value_counts_2[v]:
                        count_mismatches.append((v, value_counts_1[v], value_counts_2[v]))
                if count_mismatches:
                    logger.log(f"⚠️  Count mismatches for column '{col}':")
                    for v, c1, c2 in count_mismatches:
                        logger.log(f"    Value '{v}': {TABLE_1} has {c1}, {TABLE_2} has {c2}")
                if not missing_in_2 and not missing_in_1 and not count_mismatches:
                    logger.log(f"✅ All values and counts match for column '{col}'.")

            logger.log(f"\n📝 Log saved to {log_path}")

    logger.close()

if __name__ == "__main__":
    compare_tables()
