# Loan Default Prediction: Medallion Architecture Data Pipeline

This project implements a complete data processing pipeline following the Medallion Architecture (Bronze, Silver, Gold layers) to prepare data for loan default prediction. The system ingests, transforms, and structures data from multiple sources to build a comprehensive feature table for machine learning.

## Architecture Overview

The project follows the Medallion Architecture pattern:

- **Bronze Layer**: Raw data ingestion with minimal processing
- **Silver Layer**: Cleaned, validated, and structured data 
- **Gold Layer**: Business-level features and labels for machine learning

## Data Sources

The pipeline processes data from four primary sources:

1. **Loan Data** (`lms_loan_daily.csv`): Contains loan transaction history
2. **Customer Attributes** (`features_attributes.csv`): Contains demographic information
3. **Financial Data** (`features_financials.csv`): Contains financial indicators
4. **Clickstream Data** (`feature_clickstream.csv`): Contains customer behavior data

## Project Structure

```
medallion-pipeline/
├── data/                   # Source data files
│   ├── lms_loan_daily.csv
│   ├── features_attributes.csv
│   ├── features_financials.csv
│   └── feature_clickstream.csv
├── utils/                  # Utility functions
│   ├── __init__.py
│   ├── spark_utils.py      # Spark session management
│   ├── bronze_utils.py     # Bronze layer processing
│   └── silver_utils.py     # Silver layer processing
├── datamart/               # Output directory (created at runtime)
│   ├── bronze/             # Bronze layer tables
│   ├── silver/             # Silver layer tables
│   └── gold/               # Gold layer tables
├── main.py                 # Main pipeline script
├── Dockerfile              # Docker configuration
├── docker-compose.yaml     # Docker Compose configuration
└── requirements.txt        # Python dependencies
```

## Setup and Installation

### Prerequisites

- Docker and Docker Compose
- Git

### Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/medallion-pipeline.git
   cd medallion-pipeline
   ```

2. Place data files in the `data/` directory.

3. Build and start the Docker container:
   ```bash
   docker-compose build
   docker-compose up
   ```

4. The Jupyter Lab interface will be available at: http://localhost:8888

## Running the Pipeline

You can run the entire pipeline or specific layers using the main script:

```bash
# Run the complete pipeline (all layers)
python main.py

# Run only the Bronze layer
python main.py --layer bronze

# Run only the Silver layer
python main.py --layer silver

# Run only the Gold layer
python main.py --layer gold

# Process specific tables in the Silver layer
python main.py --layer silver --table loan
python main.py --layer silver --table attributes
python main.py --layer silver --table financials
python main.py --layer silver --table clickstream

# Only create directory structure without processing
python main.py --create-dirs
```

## Docker Commands

If you're in the Docker container, use these commands:

```bash
# Run the pipeline inside the Docker container
docker-compose exec pyspark python main.py

# Run specific layers inside the Docker container
docker-compose exec pyspark python main.py --layer silver
```

## Output

The pipeline creates several datasets:

1. **Bronze Layer**:
   - `bronze_loan_daily`: Raw loan data
   - `bronze_attributes`: Raw customer attributes
   - `bronze_financials`: Raw financial data
   - `bronze_clickstream`: Raw clickstream data

2. **Silver Layer**:
   - `silver_loan`: Processed loan data with payment status and default flags
   - `silver_attributes`: Cleaned attributes with standardized fields
   - `silver_financials`: Processed financial data with standardized indicators
   - `silver_clickstream_detailed`: Detailed clickstream data with normalization
   - `silver_clickstream_aggregated`: Aggregated clickstream features per customer

3. **Gold Layer**:
   - `gold_features`: Final feature table with integrated data from all sources

## Machine Learning Pipeline

The feature table created in the Gold layer is designed for loan default prediction, with:
- Comprehensive features from multiple data sources
- Default labels based on consecutive missed payments
- Default risk score combining multiple risk factors

## License

[MIT License](LICENSE)

## Acknowledgments

This project uses the Medallion Architecture pattern popularized by Databricks for structured data processing.
