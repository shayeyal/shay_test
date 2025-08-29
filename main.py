#!/usr/bin/env python3
"""
Main script for running the Upstream Vehicle Data Pipeline.

This script provides a command-line interface for running the complete data pipeline
or individual stages (Bronze, Silver, Gold).
"""

import logging
import argparse
import json
import sys
from pathlib import Path

# Add src directory to Python path
sys.path.append(str(Path(__file__).parent / "src"))

from src.pipeline_orchestrator import PipelineOrchestrator, PipelineConfig


def setup_logging(log_level: str = "INFO") -> None:
    """
    Setup logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('pipeline.log')
        ]
    )


def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(
        description="Upstream Vehicle Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --stage full                    # Run complete pipeline
  python main.py --stage bronze --batch-size 5000  # Run only bronze stage
  python main.py --stage silver                  # Run only silver stage  
  python main.py --stage gold                    # Run only gold stage
  python main.py --stats                         # Show pipeline statistics
        """
    )
    
    parser.add_argument(
        "--stage",
        choices=["full", "bronze", "silver", "gold"],
        default="full",
        help="Pipeline stage to run (default: full)"
    )
    
    parser.add_argument(
        "--batch-size", 
        type=int,
        default=10000,
        help="Number of messages to fetch from API (default: 10000)"
    )
    
    parser.add_argument(
        "--api-url",
        default="http://localhost:9900", 
        help="Base URL of the Upstream API (default: http://localhost:9900)"
    )
    
    parser.add_argument(
        "--batch-id",
        help="Custom batch identifier for the pipeline run"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show pipeline statistics and exit"
    )
    
    parser.add_argument(
        "--config",
        help="Path to JSON configuration file"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger("main")
    
    try:
        # Load configuration
        config = PipelineConfig(
            api_base_url=args.api_url,
            batch_size=args.batch_size
        )
        
        # Override with config file if provided
        if args.config:
            with open(args.config, 'r') as f:
                config_data = json.load(f)
                for key, value in config_data.items():
                    if hasattr(config, key):
                        setattr(config, key, value)
        
        # Initialize orchestrator
        orchestrator = PipelineOrchestrator(config)
        
        # Show stats and exit if requested
        if args.stats:
            logger.info("Retrieving pipeline statistics...")
            stats = orchestrator.get_pipeline_stats()
            print(json.dumps(stats, indent=2, default=str))
            return
            
        # Run pipeline stage
        logger.info(f"Starting pipeline stage: {args.stage}")
        
        if args.stage == "full":
            results = orchestrator.run_full_pipeline(batch_id=args.batch_id)
        elif args.stage == "bronze":
            results = orchestrator.run_bronze_stage(batch_id=args.batch_id)
        elif args.stage == "silver":
            results = orchestrator.run_silver_stage()
        elif args.stage == "gold":
            results = orchestrator.run_gold_stage()
        else:
            raise ValueError(f"Unknown stage: {args.stage}")
            
        # Print results
        print("\n" + "="*50)
        print("PIPELINE EXECUTION RESULTS")
        print("="*50)
        print(json.dumps(results, indent=2, default=str))
        
        if results["status"] == "success":
            logger.info("Pipeline completed successfully!")
            sys.exit(0)
        else:
            logger.error("Pipeline failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
