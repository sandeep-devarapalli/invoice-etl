import json
import duckdb
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class DatabaseHelper:
    def __init__(self, db_path="data/invoices.db"):
        self.db_path = db_path
        self.initialize_db()

    def initialize_db(self):
        """Initialize the database and create required tables"""
        try:
            conn = duckdb.connect(self.db_path)
            
            # Create sequence for auto-incrementing ID
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS seq_invoice_id START 1
            """)
            
            # Create invoices table with updated schema
            conn.execute("""
                CREATE TABLE IF NOT EXISTS invoices (
                    id INTEGER PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ingestion_timestamp TIMESTAMP,
                    invoice_file_name VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    json_extract TEXT,
                    completed_timestamp TIMESTAMP
                )
            """)
            
            logger.info("Database initialized successfully")
            conn.close()
        except Exception as e:
            logger.error(f"Database initialization failed: {str(e)}")
            raise e

    def load_data_batch(self, arrow_table):
        """Load batch data from Arrow table into DuckDB"""
        try:
            logger.info("Loading batch data")
            conn = duckdb.connect(self.db_path)
            
            # Create temporary table from Arrow data
            conn.execute("CREATE TEMP TABLE IF NOT EXISTS temp_invoices AS SELECT * FROM arrow_table")
            
            # Insert data into main table
            conn.execute("""
                INSERT INTO invoices (
                    id,
                    timestamp,
                    ingestion_timestamp,
                    invoice_file_name,
                    status,
                    json_extract,
                    completed_timestamp
                )
                SELECT 
                    nextval('seq_invoice_id'),
                    CURRENT_TIMESTAMP,
                    ingestion_timestamp,
                    invoice_file_name,
                    status,
                    json_extract,
                    completed_timestamp
                FROM temp_invoices
            """)
            
            conn.close()
            logger.info("Batch data loaded successfully")
            return True
        except Exception as e:
            logger.error(f"Batch data loading failed: {str(e)}")
            return False
        
    def get_db_contents(self):
        """Get current database contents as a pandas DataFrame"""
        try:
            conn = duckdb.connect(self.db_path)
            df = conn.execute("""
                SELECT 
                    id,
                    timestamp,
                    invoice_file_name,
                    status,
                    json_extract
                FROM invoices
                ORDER BY timestamp DESC
            """).df()
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Failed to fetch DB contents: {str(e)}")
            return pd.DataFrame()