"""
Bronze Writer for saving raw API data to the Bronze layer of the data lake.
"""
import pandas as pd
import duckdb
import logging
from typing import List, Dict
from datetime import datetime
import shutil
from pathlib import Path


class BronzeWriter:
    """Writes raw API data to the Bronze layer using DuckDB and Parquet format."""
    
    def __init__(self, bronze_path: str = "data/bronze"):
        """
        Initialize the Bronze Writer.
        
        Args:
            bronze_path: Path to the bronze data directory
        """
        self.bronze_path = Path(bronze_path)
        self.bronze_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def write_raw_data(self, data: List[Dict], batch_id: str = None) -> str:
        """
        Write raw vehicle message data to Bronze layer as Parquet.
        
        Args:
            data: List of vehicle message dictionaries from API
            batch_id: Optional batch identifier for the data
            
        Returns:
            Base directory path where partitioned parquet files were written
            
        Raises:
            Exception: If writing fails
        """
        if not data:
            raise ValueError("No data provided to write")
            
        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Add metadata
            current_time = datetime.utcnow()
            df['ingestion_timestamp'] = current_time.isoformat()
            df['batch_id'] = batch_id or f"batch_{current_time.strftime('%Y%m%d_%H%M%S')}"
            
            # Convert timestamp to datetime for partitioning
            df['message_datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df['partition_date'] = df['message_datetime'].dt.strftime('%Y-%m-%d')
            df['partition_hour'] = df['message_datetime'].dt.strftime('%H')
            
            # Use DuckDB for efficient partitioned writing
            conn = duckdb.connect()
            
            # Register DataFrame as table
            conn.register('raw_data', df)
            
            self.logger.info(f"Writing {len(df)} records to Bronze layer")
            
            # Create/clean output directory: if data exists under output folder, remove it before writing
            vehicle_messages_dir = self.bronze_path / "vehicle_messages"
            if vehicle_messages_dir.exists():
                self.logger.info(f"Cleaning output folder before write: {vehicle_messages_dir}")
                shutil.rmtree(vehicle_messages_dir, ignore_errors=True)
            vehicle_messages_dir.mkdir(parents=True, exist_ok=True)
            output_dir = vehicle_messages_dir

            conn.execute(f"""
                COPY (
                    SELECT 
                        vin,
                        manufacturer,
                        year,
                        model,
                        latitude,
                        longitude,
                        timestamp,
                        message_datetime,
                        velocity,
                        frontLeftDoorState as front_left_door_state,
                        wipersState as wipers_state,
                        gearPosition as gear_position,
                        driverSeatbeltState as driver_seatbelt_state,
                        fetch_timestamp,
                        ingestion_timestamp,
                        batch_id,
                        partition_date,
                        partition_hour
                    FROM raw_data
                ) TO '{output_dir}' (FORMAT PARQUET, PARTITION_BY (partition_date, partition_hour))
            """)
            
            conn.close()
            
            self.logger.info(f"Successfully wrote partitioned data under {output_dir}")
            return str(output_dir)
            
        except Exception as e:
            self.logger.error(f"Failed to write bronze data: {e}")
            raise
            
    def list_bronze_files(self) -> List[str]:
        """
        List all parquet files in the Bronze layer.
        
        Returns:
            List of parquet file paths
        """
        parquet_files = []
        bronze_dir = self.bronze_path / "vehicle_messages"
        
        if bronze_dir.exists():
            for parquet_file in bronze_dir.rglob("*.parquet"):
                parquet_files.append(str(parquet_file))
                
        self.logger.info(f"Found {len(parquet_files)} parquet files in Bronze layer")
        return parquet_files
        
    def get_bronze_stats(self) -> Dict:
        """
        Get statistics about data in the Bronze layer.
        
        Returns:
            Dictionary with statistics
        """
        try:
            files = self.list_bronze_files()
            if not files:
                return {"total_files": 0, "total_records": 0}
                
            conn = duckdb.connect()
            
            # Read all parquet files to get stats
            files_pattern = str(self.bronze_path / "vehicle_messages" / "**" / "*.parquet")
            
            result = conn.execute(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT vin) as unique_vehicles,
                    MIN(message_datetime) as earliest_message,
                    MAX(message_datetime) as latest_message
                FROM read_parquet('{files_pattern}')
            """).fetchone()
            
            conn.close()
            
            stats = {
                "total_files": len(files),
                "total_records": result[0],
                "unique_vehicles": result[1], 
                "earliest_message": result[2],
                "latest_message": result[3]
            }
            
            self.logger.info(f"Bronze layer stats: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get bronze stats: {e}")
            return {"error": str(e)}
