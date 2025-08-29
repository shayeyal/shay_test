"""
Gold Reporter for generating business reports from Silver layer data.
"""
import duckdb
import logging
from typing import Dict, Optional, List
from pathlib import Path
from datetime import datetime


class GoldReporter:
    """Generates business reports from Silver layer data and stores them in Gold layer."""
    
    def __init__(self, silver_path: str = "data/silver", gold_path: str = "data/gold"):
        """
        Initialize the Gold Reporter.
        
        Args:
            silver_path: Path to the silver data directory
            gold_path: Path to the gold data directory
        """
        self.silver_path = Path(silver_path)
        self.gold_path = Path(gold_path)
        self.gold_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def generate_vin_last_state_report(self, silver_file_path: str) -> str:
        """
        Generate VIN last state report with latest non-null values.
        
        Args:
            silver_file_path: Path to the silver parquet file
            
        Returns:
            Path to the generated gold report file
            
        Raises:
            Exception: If report generation fails
        """
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
                            velocity,
                            latitude,
                            longitude,
                            manufacturer,
                            year,
                            model,
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
                        lm.manufacturer,
                        lm.year,
                        lm.model,
                        lm.latitude as last_latitude,
                        lm.longitude as last_longitude,
                        lm.velocity as last_velocity,
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
            
            # Get report stats
            stats = conn.execute(f"""
                SELECT 
                    COUNT(*) as total_vehicles,
                    COUNT(last_front_left_door_state) as vehicles_with_door_state,
                    COUNT(last_wipers_state) as vehicles_with_wipers_state,
                    COUNT(CASE WHEN last_front_left_door_state = 'LOCKED' THEN 1 END) as locked_doors,
                    COUNT(CASE WHEN last_wipers_state = true THEN 1 END) as active_wipers,
                    AVG(last_velocity) as avg_last_velocity
                FROM read_parquet('{output_path}')
            """).fetchone()
            
            conn.close()
            
            report_stats = {
                "total_vehicles": stats[0],
                "vehicles_with_door_state": stats[1],
                "vehicles_with_wipers_state": stats[2],
                "locked_doors": stats[3],
                "active_wipers": stats[4],
                "avg_last_velocity": round(stats[5], 2) if stats[5] else None
            }
            
            self.logger.info(f"VIN last state report generated. Stats: {report_stats}")
            self.logger.info(f"Report saved to: {output_path}")
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"Failed to generate VIN last state report: {e}")
            raise
            
    def generate_manufacturer_summary_report(self, silver_file_path: str) -> str:
        """
        Generate manufacturer summary report.
        
        Args:
            silver_file_path: Path to the silver parquet file
            
        Returns:
            Path to the generated manufacturer summary report file
        """
        try:
            conn = duckdb.connect()
            
            processing_timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            output_path = str(self.gold_path / f"manufacturer_summary_{processing_timestamp}.parquet")
            
            self.logger.info("Generating manufacturer summary report")
            
            conn.execute(f"""
                COPY (
                    SELECT 
                        manufacturer,
                        COUNT(DISTINCT vin) as unique_vehicles,
                        COUNT(*) as total_messages,
                        AVG(velocity) as avg_velocity,
                        MAX(velocity) as max_velocity,
                        COUNT(CASE WHEN front_left_door_state = 'LOCKED' THEN 1 END) as locked_door_messages,
                        COUNT(CASE WHEN wipers_state = true THEN 1 END) as active_wipers_messages,
                        MIN(message_datetime) as earliest_message,
                        MAX(message_datetime) as latest_message,
                        '{processing_timestamp}' as report_generated_timestamp
                    FROM read_parquet('{silver_file_path}')
                    WHERE manufacturer IS NOT NULL
                    GROUP BY manufacturer
                    ORDER BY unique_vehicles DESC, total_messages DESC
                ) TO '{output_path}' (FORMAT PARQUET)
            """)
            
            conn.close()
            
            self.logger.info(f"Manufacturer summary report saved to: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Failed to generate manufacturer summary report: {e}")
            raise
            
    def generate_velocity_analysis_report(self, silver_file_path: str) -> str:
        """
        Generate velocity analysis report.
        
        Args:
            silver_file_path: Path to the silver parquet file
            
        Returns:
            Path to the generated velocity analysis report file
        """
        try:
            conn = duckdb.connect()
            
            processing_timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            output_path = str(self.gold_path / f"velocity_analysis_{processing_timestamp}.parquet")
            
            self.logger.info("Generating velocity analysis report")
            
            conn.execute(f"""
                COPY (
                    WITH velocity_stats AS (
                        SELECT 
                            vin,
                            manufacturer,
                            COUNT(*) as message_count,
                            AVG(velocity) as avg_velocity,
                            MIN(velocity) as min_velocity,
                            MAX(velocity) as max_velocity,
                            STDDEV(velocity) as velocity_stddev,
                            COUNT(CASE WHEN velocity = 0 THEN 1 END) as stationary_count,
                            COUNT(CASE WHEN velocity > 100 THEN 1 END) as high_speed_count,
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY velocity) as median_velocity
                        FROM read_parquet('{silver_file_path}')
                        WHERE velocity IS NOT NULL AND has_valid_velocity = true
                        GROUP BY vin, manufacturer
                    )
                    SELECT 
                        *,
                        CASE 
                            WHEN avg_velocity = 0 THEN 'Always Stationary'
                            WHEN avg_velocity < 30 THEN 'Low Speed'
                            WHEN avg_velocity < 80 THEN 'Moderate Speed'
                            ELSE 'High Speed'
                        END as velocity_category,
                        '{processing_timestamp}' as report_generated_timestamp
                    FROM velocity_stats
                    ORDER BY avg_velocity DESC
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
        
    def get_gold_stats(self) -> Dict:
        """
        Get statistics about all Gold layer reports.
        
        Returns:
            Dictionary with statistics for each report type
        """
        try:
            reports = self.list_gold_reports()
            stats = {
                "total_reports": len(reports),
                "report_types": {},
                "latest_reports": {}
            }
            
            # Categorize reports by type
            for report in reports:
                filename = Path(report).stem
                if filename.startswith("vin_last_state"):
                    report_type = "vin_last_state"
                elif filename.startswith("manufacturer_summary"):
                    report_type = "manufacturer_summary"
                elif filename.startswith("velocity_analysis"):
                    report_type = "velocity_analysis"
                else:
                    report_type = "other"
                    
                if report_type not in stats["report_types"]:
                    stats["report_types"][report_type] = 0
                stats["report_types"][report_type] += 1
                
                # Track latest report of each type
                if report_type not in stats["latest_reports"]:
                    stats["latest_reports"][report_type] = report
                elif report > stats["latest_reports"][report_type]:
                    stats["latest_reports"][report_type] = report
                    
            self.logger.info(f"Gold layer stats: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get gold stats: {e}")
            return {"error": str(e)}
