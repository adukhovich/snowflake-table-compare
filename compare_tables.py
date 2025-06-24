import json
import snowflake.connector
from typing import List
from datetime import datetime

# CONFIG
TABLE_1 = "DB1.SCHEMA1.TABLE1"
TABLE_2 = "DB2.SCHEMA2.TABLE2"

# Optional WHERE clauses to filter each table (or leave as "")
WHERE_1 = ""  # e.g., "WHERE _file = 'abc_2023-10-01.csv"
WHERE_2 = ""

EXCLUDED_COLUMNS = ['METADATA_FILENAME', 'METADATA_FILE_ROW_NUMBER', '_DBT_COPIED_AT', '_FILE', '_FIVETRAN_SYNCED', '_LINE', '_MODIFIED']
CONFIG_PATH = 'config.json'

class Logger:
    def __init__(self, filepath):
        self.filepath = filepath
        self.buffer = []

    def log(self, message):
        print(message)
        self.buffer.append(message + "\n")

    def close(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            f.writelines(self.buffer)

def create_log_file():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"compare_log_{timestamp}.txt"

def load_config(path=CONFIG_PATH):
    with open(path, 'r') as f:
        return json.load(f)

def connect_to_snowflake(config):
    return snowflake.connector.connect(
        user=config["user"],
        account=config["account"],
        warehouse=config["warehouse"],
        authenticator="externalbrowser"
    )

def get_columns(cursor, table_name: str, excluded: List[str]) -> List[str]:
    cursor.execute(f"DESC TABLE {table_name}")
    return [row[0] for row in cursor.fetchall() if row[0] not in excluded]

def get_value_counts(cursor, table_name: str, column: str, where_clause: str) -> dict:
    query = f'''
        SELECT CAST("{column}" AS STRING), COUNT(*) 
        FROM {table_name}
        {where_clause}
        GROUP BY "{column}"
    '''
    cursor.execute(query)
    return {row[0]: row[1] for row in cursor.fetchall()}

def compare_tables():
    config = load_config()
    log_path = create_log_file()
    logger = Logger(log_path)

    with connect_to_snowflake(config) as conn:
        with conn.cursor() as cursor:
            logger.log(f"\n🔍 Comparing tables:")
            logger.log(f"    • Table 1: {TABLE_1} {WHERE_1 or ''}")
            logger.log(f"    • Table 2: {TABLE_2} {WHERE_2 or ''}\n")

            # Row count comparison
            cursor.execute(f"SELECT COUNT(*) FROM {TABLE_1} {WHERE_1}")
            row_count_1 = cursor.fetchone()[0]
            cursor.execute(f"SELECT COUNT(*) FROM {TABLE_2} {WHERE_2}")
            row_count_2 = cursor.fetchone()[0]
            logger.log(f"🔢 Row count in {TABLE_1}: {row_count_1}")
            logger.log(f"🔢 Row count in {TABLE_2}: {row_count_2}")
            if row_count_1 != row_count_2:
                logger.log("❌ Tables do NOT have the same number of rows.\n")
            else:
                logger.log("✅ Both tables have the same number of rows.\n")

            # Column comparison
            cols_1 = get_columns(cursor, TABLE_1, EXCLUDED_COLUMNS)
            cols_2 = get_columns(cursor, TABLE_2, EXCLUDED_COLUMNS)

            set_1, set_2 = set(cols_1), set(cols_2)
            only_in_1 = sorted(set_1 - set_2)
            only_in_2 = sorted(set_2 - set_1)
            # Preserve ordinal position from TABLE_1
            common = [col for col in cols_1 if col in set_2]

            logger.log(f"📌 Columns only in {TABLE_1}: {only_in_1 or 'None'}")
            logger.log(f"📌 Columns only in {TABLE_2}: {only_in_2 or 'None'}\n")

            # Value-level comparison for all common columns
            logger.log(f"\n🔬 Value-level comparison for {len(common)} common columns:")
            for col in common:
                value_counts_1 = get_value_counts(cursor, TABLE_1, col, WHERE_1)
                value_counts_2 = get_value_counts(cursor, TABLE_2, col, WHERE_2)

                keys_1 = set(value_counts_1)
                keys_2 = set(value_counts_2)

                missing_in_2 = sorted(keys_1 - keys_2)
                missing_in_1 = sorted(keys_2 - keys_1)

                if missing_in_2:
                    logger.log(f"⚠️  Values in {TABLE_1}.{col} but not in {TABLE_2}: {missing_in_2}")
                if missing_in_1:
                    logger.log(f"⚠️  Values in {TABLE_2}.{col} but not in {TABLE_1}: {missing_in_1}")

                shared = keys_1 & keys_2
                count_mismatches = [(v, value_counts_1[v], value_counts_2[v])
                                    for v in shared if value_counts_1[v] != value_counts_2[v]]
                if count_mismatches:
                    logger.log(f"⚠️  Count mismatches for column '{col}':")
                    for v, c1, c2 in count_mismatches:
                        logger.log(f"    Value '{v}': {TABLE_1} has {c1}, {TABLE_2} has {c2}")
                if not (missing_in_1 or missing_in_2 or count_mismatches):
                    logger.log(f"✅ All values and counts match for column '{col}'.")

            logger.log(f"\n📝 Log saved to {log_path}")
            logger.close()

if __name__ == "__main__":
    compare_tables()
