import logging
import time
from typing import Dict
from datetime import datetime

from .api_client import APIClient
from .bronze_writer import BronzeWriter
from .silver_processor import SilverProcessor
from .gold_reporter import GoldReporter
import shutil
from pathlib import Path


class PipelineConfig:
    
    def __init__(
        self,
        api_base_url: str = "http://localhost:9900",
        api_timeout: int = 30,
        batch_size: int = 10000,
        bronze_path: str = "data/bronze",
        silver_path: str = "data/silver", 
        gold_path: str = "data/gold"
    ):
        self.api_base_url = api_base_url
        self.api_timeout = api_timeout
        self.batch_size = batch_size
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.gold_path = gold_path


class PipelineOrchestrator:
    
    def __init__(self, config: PipelineConfig = None):
        self.config = config or PipelineConfig()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize pipeline components
        self.api_client = APIClient(
            base_url=self.config.api_base_url,
            timeout=self.config.api_timeout
        )
        self.bronze_writer = BronzeWriter(bronze_path=self.config.bronze_path)
        self.silver_processor = SilverProcessor(
            bronze_path=self.config.bronze_path,
            silver_path=self.config.silver_path
        )
        self.gold_reporter = GoldReporter(
            silver_path=self.config.silver_path,
            gold_path=self.config.gold_path
        )
        
    def run_full_pipeline(self, batch_id: str = None) -> Dict:
        pipeline_start_time = time.time()
        batch_id = batch_id or f"pipeline_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        self.logger.info(f"Starting full pipeline execution - Batch ID: {batch_id}")
        
        try:
            results = {
                "batch_id": batch_id,
                "start_time": datetime.utcnow().isoformat(),
                "stages": {}
            }
            
            # Stage 1: API Data Ingestion (Bronze Layer)
            self.logger.info("Stage 1: Starting API data ingestion")
            stage_start = time.time()
            
            # Health check first
            if not self.api_client.health_check():
                raise Exception("API health check failed")
                
            # Fetch data from API
            raw_data = self.api_client.fetch_vehicle_messages(amount=self.config.batch_size)
            
            # Write to Bronze layer
            bronze_file = self.bronze_writer.write_raw_data(raw_data, batch_id)
            
            results["stages"]["bronze"] = {
                "status": "success",
                "duration_seconds": round(time.time() - stage_start, 2),
                "records_fetched": len(raw_data),
                "file_path": bronze_file
            }
            
            self.logger.info(f"Stage 1 completed in {results['stages']['bronze']['duration_seconds']} seconds")
            
            # Stage 2: Data Cleaning and Standardization (Silver Layer)
            self.logger.info("Stage 2: Starting data cleaning and standardization")
            stage_start = time.time()
            
            silver_file = self.silver_processor.process_to_silver()
            results["stages"]["silver"] = {
                "status": "success",
                "duration_seconds": round(time.time() - stage_start, 2),
                "file_path": silver_file
            }
            
            self.logger.info(f"Stage 2 completed in {results['stages']['silver']['duration_seconds']} seconds")
            
            # Stage 3: Report Generation (Gold Layer)
            self.logger.info("Stage 3: Starting report generation")
            stage_start = time.time()
            
            # Clean gold output folder before generating reports
            gold_dir = Path(self.config.gold_path)
            if gold_dir.exists():
                self.logger.info(f"Cleaning gold output folder before write: {gold_dir}")
                shutil.rmtree(gold_dir, ignore_errors=True)
            gold_dir.mkdir(parents=True, exist_ok=True)

            # Generate all reports
            vin_report = self.gold_reporter.generate_vin_last_state_report(silver_file)
            velocity_report = self.gold_reporter.fastest_vehicles_per_hour_report(silver_file)
            # Optional DQ report: scan likely text columns for SQL-injection-like patterns (SQL-based)
            try:
                dq_report = self.gold_reporter.sql_violating_messages_report(
                    bronze_file_path=str(Path(self.config.bronze_path) / "vehicle_messages" / "**" / "*.parquet"),
                    gold_file_path=self.config.gold_path,
                    columns=["vin", "manufacturer", "model"],
                    regex_list=[r";", r"--", r"/\\*", r"\\*/", r"DROP", r"SELECT", r"INSERT", r"UPDATE", r"DELETE"]
                )
            except Exception as e:
                self.logger.warning(f"SQL injection report generation failed: {e}")
                dq_report = None
            
            results["stages"]["gold"] = {
                "status": "success", 
                "duration_seconds": round(time.time() - stage_start, 2),
                "reports_generated": {
                    "vin_last_state": vin_report,
                    "velocity_analysis": velocity_report,
                    "sql_injection_report": dq_report
                }
            }
            
            self.logger.info(f"Stage 3 completed in {results['stages']['gold']['duration_seconds']} seconds")
            
            # Pipeline completion
            total_duration = time.time() - pipeline_start_time
            results["status"] = "success"
            results["end_time"] = datetime.utcnow().isoformat()
            results["total_duration_seconds"] = round(total_duration, 2)
            
            self.logger.info(f"Full pipeline completed successfully in {total_duration:.2f} seconds")
            
            return results
            
        except Exception as e:
            error_msg = f"Pipeline failed: {e}"
            self.logger.error(error_msg)
            
            results["status"] = "failed"
            results["error"] = str(e)
            results["end_time"] = datetime.utcnow().isoformat()
            results["total_duration_seconds"] = round(time.time() - pipeline_start_time, 2)
            
            raise Exception(error_msg)
            
    def run_bronze_stage(self, batch_id: str = None) -> Dict:
        batch_id = batch_id or f"bronze_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            self.logger.info(f"Running Bronze stage - Batch ID: {batch_id}")
            
            if not self.api_client.health_check():
                raise Exception("API health check failed")
                
            raw_data = self.api_client.fetch_vehicle_messages(amount=self.config.batch_size)
            bronze_file = self.bronze_writer.write_raw_data(raw_data, batch_id)
            
            return {
                "status": "success",
                "batch_id": batch_id,
                "records_fetched": len(raw_data),
                "file_path": bronze_file
            }
            
        except Exception as e:
            self.logger.error(f"Bronze stage failed: {e}")
            return {"status": "failed", "error": str(e)}
            
    def run_silver_stage(self) -> Dict:
        try:
            self.logger.info("Running Silver stage")
            
            silver_file = self.silver_processor.process_to_silver()

            return {
                "status": "success",
                "file_path": silver_file
            }
            
        except Exception as e:
            self.logger.error(f"Silver stage failed: {e}")
            return {"status": "failed", "error": str(e)}
            
    def run_gold_stage(self, silver_file_path: str = None) -> Dict:
        try:
            self.logger.info("Running Gold stage")
            
            if not silver_file_path:
                silver_file_path = self.silver_processor.get_latest_silver_file()
                if not silver_file_path:
                    raise Exception("No silver file found")
                    
            # Clean gold output folder before generating reports
            gold_dir = Path(self.gold_reporter.gold_path)
            if gold_dir.exists():
                self.logger.info(f"Cleaning gold output folder before write: {gold_dir}")
                shutil.rmtree(gold_dir, ignore_errors=True)
            gold_dir.mkdir(parents=True, exist_ok=True)

            # Generate all reports
            vin_report = self.gold_reporter.generate_vin_last_state_report(silver_file_path)
            velocity_report = self.gold_reporter.fastest_vehicles_per_hour_report(silver_file_path)
            # Optional DQ report (SQL-based)
            try:
                dq_report = self.gold_reporter.sql_violating_messages_report(
                    bronze_file_path=str(Path(self.bronze_writer.bronze_path) / "vehicle_messages" / "**" / "*.parquet"),
                    gold_file_path=self.gold_reporter.gold_path.as_posix(),
                    columns=["vin", "manufacturer", "model"],
                    regex_list=[r";", r"--", r"/\\*", r"\\*/", r"DROP", r"SELECT", r"INSERT", r"UPDATE", r"DELETE"]
                )
            except Exception as e:
                self.logger.warning(f"SQL injection report generation failed: {e}")
                dq_report = None
            
            return {
                "status": "success",
                "reports_generated": {
                    "vin_last_state": vin_report,
                    "velocity_analysis": velocity_report,
                    "sql_injection_report": dq_report
                }
            }
            
        except Exception as e:
            self.logger.error(f"Gold stage failed: {e}")
            return {"status": "failed", "error": str(e)}
            
    
