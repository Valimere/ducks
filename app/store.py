import duckdb
import os
from config import Config
import logging


class DuckDBStore:
    def __init__(self):
        db_dir = os.path.dirname(Config.DUCKDB_PATH)
        os.makedirs(db_dir, exist_ok=True)
        self.connection = duckdb.connect(Config.DUCKDB_PATH)
        self.logger = logging.getLogger('DuckDBStore')

    def close_connection(self):
        self.connection.close()

    def ingest_parquet(self, file_path):
        try:
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS temp_billing_data AS SELECT * FROM read_parquet(?);
            """, [file_path])

            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS billing_data AS 
                SELECT * FROM temp_billing_data WHERE Line_item_line_item_type = 'Usage';
            """)

            self.connection.execute("DROP TABLE temp_billing_data")
        except Exception as e:
            self.logger.error(f"Error ingesting Parquet file: {e}")
            raise

    def query_undiscounted_cost(self, service_code):
        try:
            result = self.connection.execute("""
                SELECT SUM(Line_item_unblended_cost) AS undiscounted_cost
                FROM billing_data
                WHERE Product_servicecode = ?;
            """, [service_code]).fetchone()
            return round(result[0], 2) if result else 0.0
        except Exception as e:
            self.logger.error(f"Error querying undiscounted cost: {e}")
            raise

    def query_discounted_cost(self, service_code, discount_rate):
        try:
            result = self.connection.execute("""
                SELECT SUM(Line_item_unblended_cost * ?) AS discounted_cost
                FROM billing_data
                WHERE Product_servicecode = ?;
            """, [discount_rate, service_code]).fetchone()
            return round(result[0], 2) if result else 0.0
        except Exception as e:
            self.logger.error(f"Error querying discounted cost: {e}")
            raise

    def query_blended_discount_rate(self):
        try:
            undiscounted_total = self.connection.execute("""
                SELECT SUM(Line_item_unblended_cost)
                FROM billing_data;
            """).fetchone()[0]

            discounted_total = self.connection.execute("""
                SELECT SUM(
                    CASE 
                        WHEN Product_servicecode = 'AmazonS3' THEN Line_item_unblended_cost * 0.88
                        WHEN Product_servicecode = 'AmazonEC2' THEN Line_item_unblended_cost * 0.50
                        WHEN Product_servicecode = 'AWSDataTransfer' THEN Line_item_unblended_cost * 0.70
                        WHEN Product_servicecode = 'AWSGlue' THEN Line_item_unblended_cost * 0.95
                        WHEN Product_servicecode = 'AmazonGuardDuty' THEN Line_item_unblended_cost * 0.25
                        ELSE Line_item_unblended_cost
                    END
                ) AS discounted_cost
                FROM billing_data;
            """).fetchone()[0]

            return round(discounted_total / undiscounted_total, 4) if undiscounted_total else 0.0
        except Exception as e:
            self.logger.error(f"Error querying blended discount rate: {e}")
            raise

    def query_all_costs(self):
        try:
            results = self.connection.execute("""
                SELECT 
                    Product_servicecode, 
                    SUM(Line_item_unblended_cost) AS undiscounted_cost,
                    SUM(
                        CASE 
                            WHEN Product_servicecode = 'AmazonS3' THEN Line_item_unblended_cost * 0.88
                            WHEN Product_servicecode = 'AmazonEC2' THEN Line_item_unblended_cost * 0.50
                            WHEN Product_servicecode = 'AWSDataTransfer' THEN Line_item_unblended_cost * 0.70
                            WHEN Product_servicecode = 'AWSGlue' THEN Line_item_unblended_cost * 0.95
                            WHEN Product_servicecode = 'AmazonGuardDuty' THEN Line_item_unblended_cost * 0.25
                            ELSE Line_item_unblended_cost
                        END
                    ) AS discounted_cost
                FROM billing_data
                GROUP BY Product_servicecode;
            """).fetchall()

            return [
                {
                    "service_code": row[0],
                    "undiscounted_cost": round(row[1], 2),
                    "discounted_cost": round(row[2], 2)
                }
                for row in results
            ]
        except Exception as e:
            self.logger.error(f"Error querying all costs: {e}")
            raise
