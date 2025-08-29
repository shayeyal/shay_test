# Upstream Vehicle Data Pipeline

A comprehensive data pipeline for processing vehicle telemetry data from the Upstream API. This project implements a three-tier data lake architecture (Bronze, Silver, Gold) using DuckDB and Parquet format for efficient data processing and storage.

## Architecture

The pipeline follows a modern data lake pattern with three distinct layers:

- **Bronze Layer**: Raw data ingested directly from the API
- **Silver Layer**: Cleaned and standardized data ready for analysis  
- **Gold Layer**: Business reports and aggregated insights

## Features

- **Object-Oriented Design**: Modular, testable, and maintainable code structure
- **Efficient Data Processing**: Uses DuckDB for high-performance analytics
- **Partitioned Storage**: Data partitioned by date and hour for optimal query performance
- **Data Quality Checks**: Validates data integrity throughout the pipeline
- **Comprehensive Logging**: Detailed logging for monitoring and debugging
- **Flexible Configuration**: Configurable batch sizes, paths, and API endpoints

## Setup

1. **Create and activate virtual environment:**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

3. **Ensure the Upstream API server is running:**
```bash
docker run -p 9900:9900 upstream-interview
```

## Usage

### Run Complete Pipeline
```bash
python main.py --stage full --batch-size 10000
```

### Run Individual Stages
```bash
# Bronze layer (API ingestion)
python main.py --stage bronze --batch-size 5000

# Silver layer (data cleaning)  
python main.py --stage silver

# Gold layer (report generation)
python main.py --stage gold
```

### View Pipeline Statistics
```bash
python main.py --stats
```

### Custom Configuration
```bash
python main.py --stage full --api-url http://localhost:9900 --batch-size 15000 --log-level DEBUG
```

## Project Structure

```
├── data/                          # Data lake storage
│   ├── bronze/                    # Raw API data (partitioned parquet)
│   ├── silver/                    # Cleaned data (parquet) 
│   └── gold/                      # Business reports (parquet)
├── src/                           # Source code modules
│   ├── api_client.py              # API data fetching
│   ├── bronze_writer.py           # Raw data storage
│   ├── silver_processor.py        # Data cleaning & standardization
│   ├── gold_reporter.py           # Report generation
│   └── pipeline_orchestrator.py   # Pipeline coordination
├── main.py                        # Main execution script
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

## Data Pipeline Stages

### 1. Bronze Layer (Data Ingestion)
- Fetches raw JSON data from the Upstream API
- Adds metadata (ingestion timestamp, batch ID)
- Partitions data by date and hour
- Stores as Parquet files for efficient storage

### 2. Silver Layer (Data Processing)  
- Reads Bronze layer data using DuckDB
- Performs data cleaning:
  - Trims manufacturer names
  - Removes records with null VINs
  - Converts gear positions to numeric values
  - Validates coordinates and velocity ranges
- Adds data quality flags
- Stores cleaned data as Parquet

### 3. Gold Layer (Business Reports)
- **VIN Last State Report**: Latest status for each vehicle
- **Manufacturer Summary**: Statistics by car manufacturer  
- **Velocity Analysis**: Speed patterns and categories

## Components

### APIClient
Handles communication with the Upstream API:
- Fetches vehicle messages in configurable batches
- Implements retry logic and error handling
- Performs API health checks

### BronzeWriter  
Manages raw data storage:
- Partitions data by date and hour for optimal performance
- Uses DuckDB for efficient Parquet writing
- Provides data statistics and file management

### SilverProcessor
Handles data cleaning and standardization:
- Validates and cleans data fields
- Converts data types for consistency  
- Adds data quality metrics
- Optimizes data for analytical queries

### GoldReporter
Generates business insights:
- Creates vehicle state summaries
- Analyzes manufacturer patterns
- Provides velocity and usage analytics
- Exports reports as queryable Parquet files

### PipelineOrchestrator
Coordinates the entire pipeline:
- Manages execution flow between stages
- Handles error recovery and logging
- Provides pipeline statistics and monitoring
- Supports individual stage execution

## Data Schema

### Bronze Layer
Raw API data with additional metadata:
- All original API fields preserved
- `fetch_timestamp`: When data was retrieved
- `ingestion_timestamp`: When data was processed
- `batch_id`: Unique identifier for the batch
- Partitioning: `date` and `hour` based on message timestamp

### Silver Layer  
Cleaned and standardized data:
- Standardized field names (snake_case)
- Validated data types and ranges
- Data quality flags for each record
- `gear_position_numeric`: Numeric gear values
- `has_valid_*`: Quality indicators

### Gold Layer Reports
1. **VIN Last State**: Latest vehicle information and component states
2. **Manufacturer Summary**: Aggregated statistics by manufacturer
3. **Velocity Analysis**: Speed patterns and behavioral insights

## Configuration

The pipeline can be configured via:
- Command line arguments
- Environment variables  
- JSON configuration files
- PipelineConfig class properties

## Logging

Comprehensive logging is provided at multiple levels:
- **DEBUG**: Detailed execution information
- **INFO**: Pipeline progress and statistics  
- **WARNING**: Non-critical issues
- **ERROR**: Critical failures

Logs are written to both console and `pipeline.log` file.

## Error Handling

The pipeline implements robust error handling:
- API connection failures with retry logic
- Data validation with detailed error messages
- Graceful handling of missing or corrupt data
- Pipeline stage isolation to prevent cascading failures

## Performance

Optimized for handling large datasets:
- Partitioned storage for efficient querying
- DuckDB for high-performance analytics
- Streaming data processing to minimize memory usage
- Configurable batch sizes for different environments

## Development

### Running Tests
```bash
# Install development dependencies
pip install pytest pytest-cov

# Run tests
pytest tests/ -v --cov=src
```

### Code Quality
The codebase follows Python best practices:
- Type hints for better code documentation
- Comprehensive error handling  
- Modular design with single responsibility principle
- Extensive logging and monitoring capabilities
