import duckdb
import logging
from typing import List
from pathlib import Path
from datetime import datetime


class GoldReporter:
    
    def __init__(self, silver_path: str = "data/silver", gold_path: str = "data/gold"):
        self.silver_path = Path(silver_path)
        self.gold_path = Path(gold_path)
        self.gold_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def generate_vin_last_state_report(self, silver_file_path: str) -> str:
        try:
            conn = duckdb.connect()
            
            processing_timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            output_path = str(self.gold_path / f"vin_last_state_{processing_timestamp}.parquet")
            
            self.logger.info("Generating VIN last state report")
            
            # Generate the VIN last state report
            conn.execute(f"""
                COPY (
                    WITH latest_messages AS (
                        SELECT 
                            vin,
                            message_datetime,
                            timestamp as last_reported_timestamp,
                            front_left_door_state,
                            wipers_state,
                            ROW_NUMBER() OVER (PARTITION BY vin ORDER BY message_datetime DESC) as rn
                        FROM read_parquet('{silver_file_path}')
                        WHERE vin IS NOT NULL
                    ),
                    last_door_state AS (
                        SELECT DISTINCT
                            vin,
                            FIRST_VALUE(front_left_door_state) OVER (
                                PARTITION BY vin 
                                ORDER BY CASE WHEN front_left_door_state IS NOT NULL THEN message_datetime END DESC
                                ROWS UNBOUNDED PRECEDING
                            ) as last_front_left_door_state
                        FROM read_parquet('{silver_file_path}')
                        WHERE vin IS NOT NULL AND front_left_door_state IS NOT NULL
                    ),
                    last_wipers_state AS (
                        SELECT DISTINCT
                            vin,
                            FIRST_VALUE(wipers_state) OVER (
                                PARTITION BY vin 
                                ORDER BY CASE WHEN wipers_state IS NOT NULL THEN message_datetime END DESC
                                ROWS UNBOUNDED PRECEDING
                            ) as last_wipers_state
                        FROM read_parquet('{silver_file_path}')
                        WHERE vin IS NOT NULL AND wipers_state IS NOT NULL
                    )
                    SELECT 
                        lm.vin,
                        lm.last_reported_timestamp,
                        lm.message_datetime as last_message_datetime,
                        lds.last_front_left_door_state,
                        lws.last_wipers_state,
                        '{processing_timestamp}' as report_generated_timestamp
                    FROM latest_messages lm
                    LEFT JOIN last_door_state lds ON lm.vin = lds.vin
                    LEFT JOIN last_wipers_state lws ON lm.vin = lws.vin
                    WHERE lm.rn = 1
                    ORDER BY lm.message_datetime DESC
                ) TO '{output_path}' (FORMAT PARQUET)
            """)
            
            conn.close()
            
            self.logger.info("VIN last state report generated.")
            self.logger.info(f"Report saved to: {output_path}")
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"Failed to generate VIN last state report: {e}")
            raise
            
    def fastest_vehicles_per_hour_report(self, silver_file_path: str) -> str:
        try:
            conn = duckdb.connect()
            
            processing_timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            output_path = str(self.gold_path / f"velocity_analysis_{processing_timestamp}.parquet")
            
            self.logger.info("Generating velocity analysis report")
            
            conn.execute(f"""
                COPY (
                    WITH ranked AS (
                        SELECT 
                            CAST(message_datetime AS DATE) AS date_part,
                            EXTRACT('hour' FROM message_datetime) AS hour_part,
                            velocity,
                            ROW_NUMBER() OVER (
                                PARTITION BY CAST(message_datetime AS DATE), EXTRACT('hour' FROM message_datetime)
                                ORDER BY velocity DESC
                            ) AS rn
                        FROM read_parquet('{silver_file_path}')
                        WHERE velocity IS NOT NULL
                    )
                    SELECT 
                        date_part,
                        hour_part,
                        velocity,
                        '{processing_timestamp}' as report_generated_timestamp
                    FROM ranked
                    WHERE rn <= 10
                    ORDER BY date_part, hour_part, velocity DESC
                ) TO '{output_path}' (FORMAT PARQUET)
            """)
            
            conn.close()
            
            self.logger.info(f"Velocity analysis report saved to: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Failed to generate velocity analysis report: {e}")
            raise
            
    def list_gold_reports(self) -> List[str]:
        """
        List all report files in the Gold layer.
        
        Returns:
            List of report file paths
        """
        report_files = []
        for parquet_file in self.gold_path.glob("*.parquet"):
            report_files.append(str(parquet_file))
            
        self.logger.info(f"Found {len(report_files)} report files in Gold layer")
        return sorted(report_files)
        
    
    def sql_violating_messages_report(self, bronze_file_path: str, gold_file_path: str, columns: list[str], regex_list: list[str]):
        con = duckdb.connect()

        processing_timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        output_path = str(self.gold_path / f"sql_violating_messages_report{processing_timestamp}.parquet")
        # Build WHERE conditions dynamically
        conditions = []
        for col in columns:
            for regex in regex_list:
                conditions.append(f"REGEXP_MATCHES(CAST({col} AS VARCHAR), '{regex}')")

        where_clause = " OR ".join(conditions)

        query = f"""
            COPY (
                SELECT *, 
                    CASE
                        {" ".join([f"WHEN REGEXP_MATCHES(CAST({col} AS VARCHAR), '{regex}') THEN '{col}'"
                                    for col in columns for regex in regex_list])}
                    END AS violating_column
                FROM read_parquet('{bronze_file_path}')
                WHERE {where_clause}
            ) TO '{output_path}' (FORMAT 'parquet');
        """

        con.execute(query)
        con.close()

