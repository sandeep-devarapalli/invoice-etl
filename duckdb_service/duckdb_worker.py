import duckdb
import time
import os

def initialize_database():
    """Initialize the DuckDB database and create required tables"""
    try:
        db_path = os.getenv('DUCKDB_PATH', '/data/invoices.db')
        conn = duckdb.connect(db_path)
        
        # Create the invoices table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS invoices (
                invoice_id STRING,
                status STRING,
                vendor_info STRING,
                total_amount FLOAT,
                tax_details STRING,
                line_items STRING,
                json_extract STRING
            )
        """)
        
        conn.close()
        print(f"Database initialized successfully at {db_path}")
        
    except Exception as e:
        print(f"Error initializing database: {e}")
        raise e

if __name__ == "__main__":
    # Initialize the database
    initialize_database()