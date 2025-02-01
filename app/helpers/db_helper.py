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
            logger.info("Database initialized successfully")
            conn.close()
        except Exception as e:
            logger.error(f"Database initialization failed: {str(e)}")
            raise e

    def check_db_status(self):
        """Check database status and return table info"""
        try:
            conn = duckdb.connect(self.db_path)
            columns = conn.execute("DESCRIBE invoices").fetchall()
            row_count = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
            conn.close()
            return {
                "table_exists": True,
                "columns": columns,
                "row_count": row_count
            }
        except Exception as e:
            logger.error(f"Database status check failed: {str(e)}")
            return {"error": str(e)}

    def load_data(self, transformed_data):
        """Load transformed data into DuckDB"""
        try:
            logger.info(f"Attempting to load data: {transformed_data}")
            conn = duckdb.connect(self.db_path)
            conn.execute("""
                INSERT INTO invoices (
                    invoice_id, status, vendor_info, total_amount, 
                    tax_details, line_items, json_extract
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, transformed_data)
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
            # First get the data with simpler column names
            df = conn.execute("""
                SELECT 
                    invoice_id,
                    status,
                    vendor_info,
                    total_amount,
                    tax_details,
                    line_items,
                    json_extract
                FROM invoices
            """).df()
            conn.close()
                
            if not df.empty:
                try:
                    # Format the Total Amount column
                    df["total_amount"] = df["total_amount"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "")
                    
                    # Safely convert JSON strings to readable format
                    def safe_json_parse(x):
                        try:
                            if pd.isna(x) or x == '':
                                return ''
                            parsed = json.loads(x)
                            return str(parsed) if parsed else ''
                        except:
                            return str(x)
                    
                    df["tax_details"] = df["tax_details"].apply(safe_json_parse)
                    df["line_items"] = df["line_items"].apply(safe_json_parse)
                    
                    # Rename columns for display
                    df.columns = [
                        "Invoice ID",
                        "Status",
                        "Vendor Info",
                        "Total Amount",
                        "Tax Details",
                        "Line Items",
                        "JSON Extract"
                    ]
                except Exception as e:
                    logger.error(f"Error formatting data: {str(e)}")
                    
            return df
                
        except Exception as e:
            logger.error(f"Failed to fetch DB contents: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame