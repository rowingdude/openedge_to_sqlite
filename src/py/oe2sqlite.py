#!/usr/bin/env python3
import os
import jaydebeapi
import sqlite3
from datetime import datetime
import logging
import time
import argparse
from typing import Dict, Any, List, Optional
from config import Config

class SyncState:
    def __init__(self, sqlite_conn):
        self.sqlite_conn = sqlite_conn
        self.logger = logging.getLogger("data_import")
        self._ensure_state_table()
        
    def _ensure_state_table(self):
        cursor = self.sqlite_conn.cursor()
        try:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_state (
                table_name TEXT PRIMARY KEY,
                last_sync_time TEXT,
                last_key_value TEXT,
                sync_method TEXT DEFAULT 'timestamp',
                row_count INTEGER DEFAULT 0
            )
            """)
            self.sqlite_conn.commit()
            self.logger.info("Ensured sync state table exists")
        except Exception as e:
            self.sqlite_conn.rollback()
            self.logger.error(f"Error creating sync state table: {e}")
        finally:
            cursor.close()
    
    def get_last_sync(self, table_name: str) -> Dict[str, Any]:
        cursor = self.sqlite_conn.cursor()
        try:
            cursor.execute("""
            SELECT last_sync_time, last_key_value, sync_method, row_count
            FROM sync_state
            WHERE table_name = ?
            """, (table_name,))
            result = cursor.fetchone()
            if result:
                return {
                    "last_sync_time": result[0],
                    "last_key_value": result[1],
                    "sync_method": result[2],
                    "row_count": result[3]
                }
            return None
        except Exception as e:
            self.logger.error(f"Error retrieving sync state for {table_name}: {e}")
            return None
        finally:
            cursor.close()
    
    def update_sync_state(self, table_name: str, last_key_value: str = None, 
                         sync_method: str = 'timestamp', row_count: int = 0):
        cursor = self.sqlite_conn.cursor()
        try:
            cursor.execute("""
            INSERT OR REPLACE INTO sync_state
                (table_name, last_sync_time, last_key_value, sync_method, row_count)
            VALUES (?, datetime('now'), ?, ?, ?)
            """, (table_name, last_key_value, sync_method, row_count))
            self.sqlite_conn.commit()
            self.logger.info(f"Updated sync state for {table_name}, key: {last_key_value}, rows: {row_count}")
        except Exception as e:
            self.sqlite_conn.rollback()
            self.logger.error(f"Error updating sync state for {table_name}: {e}")
        finally:
            cursor.close()

class DataSyncManager:
    def __init__(self, full_sync: bool = False, ignore_tables: list = None):
        self.config = Config()
        self.full_sync = full_sync
        self.batch_size = self.config.mirror_settings["batch_size"]
        self.setup_logging()
        
        progress_config = self.config.progress_db
        jdbc_url = f"jdbc:datadirect:openedge://{progress_config['host']}:{progress_config['port']};databaseName={progress_config['db_name']}"
        self.oe_conn = None
        self.sqlite_conn = None
        self.ignored_tables = set()
        self.load_ignore_list()
        if ignore_tables:
            self.add_to_ignore_list(ignore_tables)
        
        self.metrics = {
            "tables_processed": 0,
            "rows_synced": 0,
            "start_time": time.time()
        }

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(self.config.mirror_settings["log_file"]),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("data_import")

    def load_ignore_list(self):
        ignore_file = self.config.mirror_settings.get("ignore_file", "ignored_tables.txt")
        try:
            if os.path.exists(ignore_file):
                with open(ignore_file, "r") as f:
                    self.ignored_tables = {line.strip().lower() for line in f if line.strip()}
                self.logger.info(f"Loaded ignore list with {len(self.ignored_tables)} tables")
        except Exception as e:
            self.logger.error(f"Error loading ignore file {ignore_file}: {e}")

    def add_to_ignore_list(self, tables: list):
        ignore_file = self.config.mirror_settings.get("ignore_file", "ignored_tables.txt")
        try:
            with open(ignore_file, "a") as f:
                for table in tables:
                    table = table.strip().lower()
                    if table not in self.ignored_tables:
                        f.write(f"{table}\n")
                        self.ignored_tables.add(table)
            self.logger.info(f"Added tables to ignore list: {tables}")
        except Exception as e:
            self.logger.error(f"Error adding tables to ignore file: {e}")

    def connect_databases(self):
        try:
            progress_config = self.config.progress_db
            jdbc_url = f"jdbc:datadirect:openedge://{progress_config['host']}:{progress_config['port']};databaseName={progress_config['db_name']}"
            self.oe_conn = jaydebeapi.connect(
                progress_config["driver_class"],
                jdbc_url,
                [progress_config["user"], progress_config["password"]],
                progress_config["jar_file"]
            )
            self.logger.info("Connected to OpenEdge database")
            
            db_path = self.config.sqlite_db.get("db_path", "analytics.db")
            self.sqlite_conn = sqlite3.connect(db_path)
            self.sqlite_conn.execute("PRAGMA foreign_keys = ON")
            self.logger.info(f"Connected to SQLite database at {db_path}")
            
            self.sync_state = SyncState(self.sqlite_conn)
            
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to databases: {e}")
            self.disconnect_databases()
            return False

    def disconnect_databases(self):
        if self.oe_conn:
            try:
                self.oe_conn.close()
                self.logger.info("Closed OpenEdge connection")
            except:
                pass
            self.oe_conn = None
            
        if self.sqlite_conn:
            try:
                self.sqlite_conn.close()
                self.logger.info("Closed SQLite connection")
            except:
                pass
            self.sqlite_conn = None

    def get_source_tables(self):
        if not self.oe_conn:
            self.logger.error("No OpenEdge connection")
            return []
            
        tables = []
        cursor = None
        
        try:
            cursor = self.oe_conn.cursor()
            metadata = self.oe_conn.jconn.getMetaData()
            result_set = metadata.getTables(None, "PUB", None, ["TABLE"])
            
            while result_set.next():
                table_name = result_set.getString("TABLE_NAME").lower()
                
                if table_name.startswith("_") or table_name in self.ignored_tables:
                    continue
                
                pk_result_set = metadata.getPrimaryKeys(None, "PUB", table_name)
                pk_column = None
                if pk_result_set.next():
                    pk_column = pk_result_set.getString("COLUMN_NAME").lower()
                pk_result_set.close()
                
                try:
                    cursor.execute(f"SELECT * FROM PUB.{table_name} WHERE 1=0")
                    columns = [desc[0].strip().lower() for desc in cursor.description]
                    
                    if columns:
                        tables.append({
                            "table_name": table_name, 
                            "columns": columns,
                            "pk_column": pk_column
                        })
                        self.logger.info(f"Found table {table_name} with {len(columns)} columns and PK: {pk_column}")
                except Exception as e:
                    self.logger.warning(f"Error getting schema for {table_name}: {e}")
                    if "permission denied" in str(e).lower():
                        self.add_to_ignore_list([table_name])
            
            return tables
        except Exception as e:
            self.logger.error(f"Error getting OpenEdge tables: {e}")
            return []
        finally:
            if cursor:
                cursor.close()

    def get_source_row_count(self, table_name: str) -> int:
        if not self.oe_conn:
            return 0
            
        cursor = None
        try:
            cursor = self.oe_conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM PUB.{table_name}")
            row = cursor.fetchone()
            count = row[0] if row else 0
            self.logger.info(f"Source table {table_name} has {count} rows")
            return count
        except Exception as e:
            self.logger.error(f"Error counting rows in {table_name}: {e}")
            return 0
        finally:
            if cursor:
                cursor.close()

    def ensure_target_table(self, table_info: Dict):
        if not self.sqlite_conn:
            return False
            
        table_name = table_info["table_name"]
        columns = table_info["columns"]
        cursor = None
        
        try:
            cursor = self.sqlite_conn.cursor()
            cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
            """, (table_name,))
            
            table_exists = bool(cursor.fetchone())
            
            if not table_exists:
                column_defs = [f'"{col}" TEXT' for col in columns]
                create_query = f"""
                CREATE TABLE {table_name} (
                    {', '.join(column_defs)}
                )
                """
                cursor.execute(create_query)
                self.sqlite_conn.commit()
                self.logger.info(f"Created table {table_name}")
                return True
            else:
                cursor.execute(f"PRAGMA table_info({table_name})")
                existing_columns = {row[1].lower() for row in cursor.fetchall()}
                source_columns = {col.lower() for col in columns}
                
                missing_columns = source_columns - existing_columns
                
                if missing_columns:
                    for column in missing_columns:
                        cursor.execute(f"""
                        ALTER TABLE {table_name} ADD COLUMN "{column}" TEXT
                        """)
                    
                    self.sqlite_conn.commit()
                    self.logger.info(f"Added {len(missing_columns)} columns to {table_name}")
                
                return True
        except Exception as e:
            self.sqlite_conn.rollback()
            self.logger.error(f"Error ensuring target table {table_name}: {e}")
            return False
        finally:
            if cursor:
                cursor.close()

    def get_sync_strategy(self, table_info: Dict) -> str:
        table_name = table_info["table_name"]
        pk_column = table_info.get("pk_column")
        
        if self.full_sync:
            return "full"
            
        last_sync = self.sync_state.get_last_sync(table_name)
        
        if not last_sync:
            return "full"
            
        if pk_column:
            return "key_based"
            
        return "timestamp"

    def sync_full_table(self, table_info: Dict) -> int:
        if not self.oe_conn or not self.sqlite_conn:
            return 0
            
        table_name = table_info["table_name"]
        columns = table_info["columns"]
        pk_column = table_info.get("pk_column")
        
        total_rows = self.get_source_row_count(table_name)
        
        oe_cursor = None
        sqlite_cursor = None
        
        try:
            sqlite_cursor = self.sqlite_conn.cursor()
            sqlite_cursor.execute(f"DELETE FROM {table_name}")
            self.sqlite_conn.commit()
            oe_cursor = self.oe_conn.cursor()
            oe_cursor.execute(f"SELECT {', '.join(f'\"{col}\"' for col in columns)} FROM PUB.{table_name}")
            rows_synced = 0
            last_value = None
            
            while True:
                batch = oe_cursor.fetchmany(self.batch_size)
                if not batch:
                    break
                    
                batch_rows = []
                for row in batch:
                    if pk_column:
                        pk_index = columns.index(pk_column)
                        if row[pk_index] is not None:
                            last_value = str(row[pk_index])
                    
                    processed_row = []
                    for value in row:
                        if value is None:
                            processed_row.append(None)
                        elif hasattr(value, 'isoformat'):
                            processed_row.append(value.isoformat())
                        else:
                            processed_row.append(str(value))
                    
                    batch_rows.append(processed_row)
                
                if batch_rows:
                    insert_query = f"""
                    INSERT INTO {table_name} 
                    ({', '.join(f'"{col}"' for col in columns)})
                    VALUES ({','.join(['?'] * len(columns))})
                    """
                    sqlite_cursor.executemany(insert_query, batch_rows)
                    self.sqlite_conn.commit()
                    
                    rows_synced += len(batch_rows)
                    
                    progress_pct = (rows_synced / total_rows * 100) if total_rows > 0 else 0
                    self.logger.info(f"Inserted {len(batch_rows)} rows for {table_name} "
                                f"(total: {rows_synced} of {total_rows} ({progress_pct:.1f}%))")
            
            if pk_column and last_value:
                self.sync_state.update_sync_state(
                    table_name, 
                    last_key_value=last_value,
                    sync_method="key_based", 
                    row_count=rows_synced
                )
            else:
                self.sync_state.update_sync_state(
                    table_name,
                    sync_method="timestamp",
                    row_count=rows_synced
                )
            
            self.logger.info(f"Completed full sync of {table_name}: {rows_synced} rows")
            return rows_synced
        except Exception as e:
            self.sqlite_conn.rollback()
            self.logger.error(f"Error performing full sync of {table_name}: {e}")
            return 0
        finally:
            if oe_cursor:
                oe_cursor.close()
            if sqlite_cursor:
                sqlite_cursor.close()

    def sync_key_based(self, table_info: Dict) -> int:
        if not self.oe_conn or not self.sqlite_conn:
            return 0
            
        table_name = table_info["table_name"]
        columns = table_info["columns"]
        pk_column = table_info.get("pk_column")
        
        if not pk_column:
            self.logger.warning(f"Table {table_name} has no primary key, falling back to full sync")
            return self.sync_full_table(table_info)
        
        last_sync = self.sync_state.get_last_sync(table_name)
        last_key_value = last_sync.get("last_key_value") if last_sync else None
        
        if not last_key_value:
            self.logger.warning(f"No last key value for {table_name}, falling back to full sync")
            return self.sync_full_table(table_info)
        
        total_new_rows = 0
        try:
            count_cursor = self.oe_conn.cursor()
            count_cursor.execute(f"SELECT COUNT(*) FROM PUB.{table_name} WHERE \"{pk_column}\" > ?", (last_key_value,))
            count_row = count_cursor.fetchone()
            total_new_rows = count_row[0] if count_row else 0
            count_cursor.close()
            
            self.logger.info(f"Found {total_new_rows} new/changed rows to sync for {table_name}")
        except Exception as e:
            self.logger.warning(f"Could not count new rows for {table_name}: {e}")
        
        oe_cursor = None
        sqlite_cursor = None
        
        try:
            oe_cursor = self.oe_conn.cursor()
            sqlite_cursor = self.sqlite_conn.cursor()
            
            query = f"""
            SELECT {', '.join(f'\"{col}\"' for col in columns)} 
            FROM PUB.{table_name} 
            WHERE "{pk_column}" > ?
            ORDER BY "{pk_column}"
            """
            
            oe_cursor.execute(query, (last_key_value,))
            
            rows_synced = 0
            last_value = last_key_value
            
            while True:
                batch = oe_cursor.fetchmany(self.batch_size)
                if not batch:
                    break
                    
                batch_rows = []
                for row in batch:
                    pk_index = columns.index(pk_column)
                    if row[pk_index] is not None:
                        last_value = str(row[pk_index])
                    
                    processed_row = []
                    for value in row:
                        if value is None:
                            processed_row.append(None)
                        elif hasattr(value, 'isoformat'):
                            processed_row.append(value.isoformat())
                        else:
                            processed_row.append(str(value))
                    
                    batch_rows.append(processed_row)
                
                if batch_rows:
                    # SQLite doesn't support ON CONFLICT, so we'll delete then insert
                    pk_values = [row[pk_index] for row in batch]
                    placeholders = ','.join(['?'] * len(pk_values))
                    sqlite_cursor.execute(f"""
                    DELETE FROM {table_name} 
                    WHERE "{pk_column}" IN ({placeholders})
                    """, pk_values)
                    
                    insert_query = f"""
                    INSERT INTO {table_name} 
                    ({', '.join(f'"{col}"' for col in columns)})
                    VALUES ({','.join(['?'] * len(columns))})
                    """
                    sqlite_cursor.executemany(insert_query, batch_rows)
                    self.sqlite_conn.commit()
                    
                    rows_synced += len(batch_rows)
                    
                    progress_pct = (rows_synced / total_new_rows * 100) if total_new_rows > 0 else 0
                    existing_rows = last_sync.get("row_count", 0)
                    
                    self.logger.info(f"Synced {len(batch_rows)} rows for {table_name} "
                                f"(total: {rows_synced} of {total_new_rows} ({progress_pct:.1f}%) - "
                                f"table total will be {existing_rows + rows_synced})")
            
            if rows_synced > 0 or last_value != last_key_value:
                self.sync_state.update_sync_state(
                    table_name, 
                    last_key_value=last_value,
                    sync_method="key_based", 
                    row_count=rows_synced + (last_sync.get("row_count") or 0)
                )
            
            self.logger.info(f"Completed key-based sync of {table_name}: {rows_synced} new/changed rows")
            return rows_synced
        except Exception as e:
            self.sqlite_conn.rollback()
            self.logger.error(f"Error performing key-based sync of {table_name}: {e}")
            return 0
        finally:
            if oe_cursor:
                oe_cursor.close()
            if sqlite_cursor:
                sqlite_cursor.close()

    def sync_timestamp_based(self, table_info: Dict) -> int:
        return self.sync_full_table(table_info)

    def sync_table(self, table_info: Dict) -> int:
        table_name = table_info["table_name"]
        
        if not self.ensure_target_table(table_info):
            self.logger.error(f"Failed to ensure target table {table_name}")
            return 0
        
        strategy = self.get_sync_strategy(table_info)
        self.logger.info(f"Using {strategy} sync strategy for {table_name}")
        
        rows_synced = 0
        if strategy == "full":
            rows_synced = self.sync_full_table(table_info)
        elif strategy == "key_based":
            rows_synced = self.sync_key_based(table_info)
        elif strategy == "timestamp":
            rows_synced = self.sync_timestamp_based(table_info)
        
        return rows_synced

    def run_sync(self):
        self.logger.info(f"Starting data sync (full_sync={self.full_sync})")
        
        start_time = time.time()
        self.metrics["start_time"] = start_time
        
        try:
            if not self.connect_databases():
                self.logger.error("Failed to connect to databases")
                return
            
            tables = self.get_source_tables()
            if not tables:
                self.logger.error("No tables found to sync")
                return
                
            self.logger.info(f"Found {len(tables)} tables to sync")
            
            for i, table_info in enumerate(tables, 1):
                table_name = table_info["table_name"]
                self.logger.info(f"Processing table {i}/{len(tables)}: {table_name}")
                
                try:
                    rows = self.sync_table(table_info)
                    
                    self.metrics["tables_processed"] += 1
                    self.metrics["rows_synced"] += rows
                    
                except Exception as e:
                    self.logger.error(f"Error syncing table {table_name}: {e}")
            
            duration = time.time() - start_time
            self.logger.info(f"Sync completed in {duration:.2f} seconds")
            self.logger.info(f"Processed {self.metrics['tables_processed']} tables")
            self.logger.info(f"Synced {self.metrics['rows_synced']} rows")
            
        except Exception as e:
            self.logger.error(f"Sync process failed: {e}")
        finally:
            self.disconnect_databases()

def main():
    parser = argparse.ArgumentParser(description="Sync data from OpenEdge to SQLite")
    parser.add_argument("--full-sync", action="store_true", help="Perform full sync of all tables")
    parser.add_argument("--ignore-table", action="append", help="Tables to ignore")
    args = parser.parse_args()
    
    syncer = DataSyncManager(full_sync=args.full_sync, ignore_tables=args.ignore_table)
    syncer.run_sync()

if __name__ == "__main__":
    main()
