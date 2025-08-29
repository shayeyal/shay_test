"""
Silver Processor for cleaning and standardizing data from Bronze to Silver layer.
"""
import duckdb
import logging
from typing import Optional
from pathlib import Path
from datetime import datetime


class SilverProcessor:
    """Processes and cleans data from Bronze layer to Silver layer."""
    
    def __init__(self, bronze_path: str = "data/bronze", silver_path: str = "data/silver"):
        """
        Initialize the Silver Processor.
        
        Args:
            bronze_path: Path to the bronze data directory
            silver_path: Path to the silver data directory
        """
        self.bronze_path = Path(bronze_path)
        self.silver_path = Path(silver_path)
        self.silver_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def process_to_silver(self) -> str:
        """
        Process Bronze layer data and write cleaned data to Silver layer.
        
        Returns:
            Path to the written silver parquet file
            
        Raises:
            Exception: If processing fails
        """
        try:
            conn = duckdb.connect()
            
            # Input pattern for bronze files
            bronze_pattern = str(self.bronze_path / "vehicle_messages" / "**" / "*.parquet")
            
            self.logger.info("Starting Silver layer processing")
            
            # Check if bronze data exists
            bronze_check = conn.execute(f"""
                SELECT COUNT(*) as count 
                FROM read_parquet('{bronze_pattern}', filename=true, hive_partitioning=false)
            """).fetchone()
            
            if bronze_check[0] == 0:
                raise ValueError("No data found in Bronze layer")
                
            self.logger.info(f"Found {bronze_check[0]} records in Bronze layer")
            
            # Define output path
            processing_timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            output_path = str(self.silver_path / f"vehicle_messages_cleaned_{processing_timestamp}.parquet")
            
            # Clean and standardize the data
            conn.execute(f"""
                COPY (
                    WITH cleaned_data AS (
                        SELECT 
                            vin,
                            TRIM(manufacturer) as manufacturer,
                            year,
                            model,
                            latitude,
                            longitude,
                            timestamp,
                            message_datetime,
                            velocity,
                            front_left_door_state,
                            wipers_state,
                            -- Convert gear positions to integers, handling special cases
                            CASE 
                                WHEN gear_position = 'NEUTRAL' THEN 0
                                WHEN gear_position = 'REVERSE' THEN -1
                                WHEN gear_position IS NULL THEN NULL
                                ELSE NULL
                            END as gear_position_numeric,
                            gear_position as gear_position_original,
                            driver_seatbelt_state,
                            fetch_timestamp,
                            ingestion_timestamp,
                            batch_id,
                            partition_date,
                            partition_hour,
                            '{processing_timestamp}' as silver_processing_timestamp
                        FROM read_parquet('{bronze_pattern}', filename=true, hive_partitioning=true)
                    )
                    SELECT * FROM cleaned_data
                    WHERE vin IS NOT NULL AND TRIM(vin) <> ''
                    ORDER BY message_datetime DESC
                ) TO '{output_path}' (FORMAT PARQUET)
            """)
            
            conn.close()
            
            self.logger.info("Silver processing completed.")
            self.logger.info(f"Silver data written to: {output_path}")
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"Failed to process silver data: {e}")
            raise
            
    def get_latest_silver_file(self) -> Optional[str]:
        """
        Get the path to the most recently created silver file.
        
        Returns:
            Path to the latest silver file, or None if no files exist
        """
        silver_files = list(self.silver_path.glob("vehicle_messages_cleaned_*.parquet"))
        
        if not silver_files:
            return None
            
        # Sort by modification time, return the most recent
        latest_file = max(silver_files, key=lambda f: f.stat().st_mtime)
        return str(latest_file)
        
    
