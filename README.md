# Climate Indicators ETL Pipeline and Forecasting

## Project Overview
This project implements a comprehensive cloud-based ETL (Extract, Transform, Load) pipeline for climate data analysis and prediction. The system automates the collection, cleaning, and aggregation of five key climate indicators:
- CO₂ levels
- Global temperature
- Ice sheets
- Sea level
- Biodiversity

The objective is to enable data-driven climate research and forecasting.

## Architecture
- **Data Source**: Google Cloud Storage (GCS) as the upstream source of truth
- **Orchestration**: Apache Airflow for workflow management
- **Data Processing**: Python-based ETL operations
- **Data Storage**: MongoDB for refined datasets
- **Modeling**: Machine learning models for climate indicator predictions

## Pipeline Components
- **Data Extraction**: Automated retrieval of climate datasets from GCS
- **Data Cleaning**: Preprocessing and standardization of raw data
- **Data Aggregation**: Creation of meaningful aggregations for analysis
- **Data Loading**: Structured storage in MongoDB for easy access
- **Predictive Modeling**: Machine learning models to forecast future climate trends

## Key Features
- Cloud-based architecture for scalability and reliability
- Automated ETL workflows with Apache Airflow
- Restructured document format in MongoDB for efficient querying
- Predictive models for CO₂ levels, global temperature, and other indicators
- Comprehensive data analysis capabilities through MongoDB aggregation pipelines

## Models and Performance
- **CO₂ level prediction**: Linear Regression model with RMSE of 2.78
- **Global temperature forecasting**: Random Forest model with RMSE of 0.47
- **Sea level and biodiversity index predictions**: ARIMA models

## Technologies Used
- **Google Cloud Platform**: Storage and authentication
- **Apache Airflow**: Workflow orchestration
- **MongoDB**: Data storage and querying
- **Python**: Data processing and modeling
- **PySpark**: Distributed computing for large datasets
- **PySpark ML**: Machine learning modeling

## Future Enhancements
- Real-time data ingestion capabilities
- Interactive visualization dashboard
- Advanced ensemble modeling techniques
- Integration with climate policy decision support systems

---

## Setup Instructions
### Prerequisites
- Python 3.x
- Google Cloud SDK
- MongoDB
- Apache Airflow
- Required Python packages (listed in `requirements.txt`)

### Installation Steps
1. Clone the repository:
   ```sh
   git clone https://github.com/your-repo/climate-etl.git
   cd climate-etl
   ```
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
3. Set up Airflow:
   ```sh
   airflow db init
   airflow scheduler & airflow webserver
   ```
4. Configure MongoDB connection in `config.yaml`
5. Run the ETL pipeline:
   ```sh
   python gcs_to_mongo_pipeline.py
   ```

## License
This project is licensed under the MIT License.
