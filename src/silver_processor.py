"""
Silver Processor for cleaning and standardizing data from Bronze to Silver layer.
"""
import duckdb
import logging
from typing import Dict, Optional
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
                            CASE 
                                WHEN latitude IS NOT NULL AND longitude IS NOT NULL 
                                     AND latitude BETWEEN -90 AND 90 
                                     AND longitude BETWEEN -180 AND 180 
                                THEN true 
                                ELSE false 
                            END as has_valid_coordinates,
                            CASE 
                                WHEN velocity IS NOT NULL AND velocity >= 0 AND velocity <= 500 
                                THEN true 
                                ELSE false 
                            END as has_valid_velocity,
                            '{processing_timestamp}' as silver_processing_timestamp
                        FROM read_parquet('{bronze_pattern}', filename=true, hive_partitioning=true)
                    )
                    SELECT * FROM cleaned_data
                    WHERE vin IS NOT NULL AND TRIM(vin) <> ''  -- Drop rows with null/empty VIN
                    ORDER BY message_datetime DESC
                ) TO '{output_path}' (FORMAT PARQUET)
            """)
            
            # Get processing stats
            stats = conn.execute(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT vin) as unique_vehicles,
                    SUM(CASE WHEN has_valid_coordinates THEN 1 ELSE 0 END) as records_with_valid_coords,
                    SUM(CASE WHEN has_valid_velocity THEN 1 ELSE 0 END) as records_with_valid_velocity,
                    COUNT(DISTINCT manufacturer) as unique_manufacturers,
                    MIN(message_datetime) as earliest_message,
                    MAX(message_datetime) as latest_message
                FROM read_parquet('{output_path}')
            """).fetchone()
            
            conn.close()
            
            processing_stats = {
                "total_records": stats[0],
                "unique_vehicles": stats[1], 
                "records_with_valid_coords": stats[2],
                "records_with_valid_velocity": stats[3],
                "unique_manufacturers": stats[4],
                "earliest_message": stats[5],
                "latest_message": stats[6]
            }
            
            self.logger.info(f"Silver processing completed. Stats: {processing_stats}")
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
        
    def get_silver_stats(self) -> Dict:
        """
        Get statistics about the latest Silver layer data.
        
        Returns:
            Dictionary with statistics
        """
        try:
            latest_file = self.get_latest_silver_file()
            if not latest_file:
                return {"error": "No silver files found"}
                
            conn = duckdb.connect()
            
            result = conn.execute(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT vin) as unique_vehicles,
                    COUNT(DISTINCT manufacturer) as unique_manufacturers,
                    AVG(CASE WHEN has_valid_velocity THEN velocity ELSE NULL END) as avg_velocity,
                    MIN(message_datetime) as earliest_message,
                    MAX(message_datetime) as latest_message,
                    SUM(CASE WHEN has_valid_coordinates THEN 1 ELSE 0 END) / COUNT(*) * 100 as valid_coords_percentage
                FROM read_parquet('{latest_file}')
            """).fetchone()
            
            conn.close()
            
            stats = {
                "file_path": latest_file,
                "total_records": result[0],
                "unique_vehicles": result[1],
                "unique_manufacturers": result[2],
                "avg_velocity": round(result[3], 2) if result[3] else None,
                "earliest_message": result[4],
                "latest_message": result[5],
                "valid_coords_percentage": round(result[6], 2) if result[6] else None
            }
            
            self.logger.info(f"Silver layer stats: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get silver stats: {e}")
            return {"error": str(e)}
