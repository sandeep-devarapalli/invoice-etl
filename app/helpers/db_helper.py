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
            logger.info(f"Initializing database at {self.db_path}")
            conn = duckdb.connect(self.db_path)
            
            # Create sequence for auto-incrementing ID
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS seq_invoice_id START 1
            """)
            
            # Create invoices table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS invoices (
                    id INTEGER PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    invoice_file_name VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    json_extract TEXT
                )
            """)
            
            logger.info("Database initialized successfully")
            conn.close()
        except Exception as e:
            logger.error(f"Database initialization failed: {str(e)}")
            raise e

    def load_data(self, file_name, status, json_extract):
        """Load data into DuckDB"""
        try:
            logger.info(f"Loading data for file: {file_name}")
            conn = duckdb.connect(self.db_path)
            conn.execute("""
                INSERT INTO invoices (id, invoice_file_name, status, json_extract)
                VALUES (nextval('seq_invoice_id'), ?, ?, ?)
            """, (file_name, status, json_extract))
            conn.close()
            logger.info("Data loaded successfully")
            return True
        except Exception as e:
            logger.error(f"Data loading failed: {str(e)}")
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