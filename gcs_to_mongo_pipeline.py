import os
import logging
import json
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
from pymongo import MongoClient

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# GCS & MongoDB Settings
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/bitterfq/.gcp/norse-fiber-450618-i5-852c60edd85b.json"
BUCKET_NAME = "globalclimateindicators"
MONGO_CONN_STRING = "mongodb+srv://climateindicators:climatemongo@usf.6ejgq.mongodb.net/"
MONGO_DB = "climate_data"

# Output directory
OUTPUT_DIR = "/tmp"

# Files to Process
FILES = [
    "co2.json",
    "global_temp.json",
    "icesheet.json",
    "living_planet_index.json",
    "sealevel.json"
]

# Initialize DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 21),
    'retries': 1
}

dag = DAG(
    'climate_data_pipeline',
    default_args=default_args,
    description='Download, clean, aggregate, and push climate data to MongoDB',
    schedule_interval=None,
    catchup=False
)

# Function: Download files from GCS
def download_from_gcs(file_name, **context):
    """Download a file from GCS"""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    
    output_path = os.path.join(OUTPUT_DIR, file_name)
    blob.download_to_filename(output_path)
    logger.info(f"✅ Downloaded {file_name} to {output_path}")
    return output_path

# Function: Clean Data
def clean_data(file_name, **context):
    """Clean the dataset and keep only relevant predictors"""
    input_path = os.path.join(OUTPUT_DIR, file_name)
    output_path = os.path.join(OUTPUT_DIR, f"clean_{file_name.replace('.json', '.csv')}")

    with open(input_path, 'r') as f:
        data = json.load(f)
    
    rows = []
    if "co2" in file_name:
        for year, year_data in data.items():
            if isinstance(year_data, dict) and "3" in year_data:
                entry = year_data["3"]
                rows.append({
                    'year': float(entry['decimal_date']),
                    'monthly_average': float(entry['monthly_average']),
                    'deseasonalized': float(entry.get('deseasonalized', 0))
                })
    
    elif "global_temp" in file_name:
        df = pd.DataFrame(data)
        rows = df[['year', 'land_ocean_temp_anomaly']].dropna().to_dict(orient='records')

    elif "icesheet" in file_name:
        df = pd.DataFrame(data)
        rows = df[['year', 'mass_balance']].dropna().to_dict(orient='records')

    elif "living_planet_index" in file_name:
        df = pd.DataFrame(data)
        rows = df[['year', 'index']].dropna().to_dict(orient='records')

    elif "sealevel" in file_name:
        df = pd.DataFrame(data)
        rows = df[['year', 'sea_level']].dropna().to_dict(orient='records')

    df_clean = pd.DataFrame(rows)
    df_clean.to_csv(output_path, index=False)
    logger.info(f"✅ Cleaned data saved to {output_path}")
    return output_path

# Function: Aggregate Data
def aggregate_data(file_name, **context):
    """Aggregate data for long-term trends"""
    input_path = os.path.join(OUTPUT_DIR, f"clean_{file_name.replace('.json', '.csv')}")
    output_path = os.path.join(OUTPUT_DIR, f"agg_{file_name.replace('.json', '.csv')}")

    df = pd.read_csv(input_path)
    if 'year' in df.columns:
        df_agg = df.groupby(df['year'].astype(int)).mean().reset_index()
        df_agg.to_csv(output_path, index=False)
        logger.info(f"✅ Aggregated data saved to {output_path}")
        return output_path

# Function: Push Data to MongoDB
def push_to_mongo(file_name, **context):
    """Push cleaned & aggregated data to MongoDB"""
    client = MongoClient(MONGO_CONN_STRING)
    db = client[MONGO_DB]

    clean_path = os.path.join(OUTPUT_DIR, f"clean_{file_name.replace('.json', '.csv')}")
    agg_path = os.path.join(OUTPUT_DIR, f"agg_{file_name.replace('.json', '.csv')}")

    # Insert Clean Data
    df_clean = pd.read_csv(clean_path)
    db[file_name.replace('.json', '_clean')].insert_many(df_clean.to_dict(orient='records'))
    logger.info(f"✅ Pushed cleaned {file_name} to MongoDB")

    # Insert Aggregated Data
    df_agg = pd.read_csv(agg_path)
    db[file_name.replace('.json', '_agg')].insert_many(df_agg.to_dict(orient='records'))
    logger.info(f"✅ Pushed aggregated {file_name} to MongoDB")

# Create Tasks Dynamically for Each File
for file in FILES:
    download_task = PythonOperator(
        task_id=f"download_{file.replace('.json', '')}",
        python_callable=download_from_gcs,
        op_kwargs={"file_name": file},
        dag=dag,
    )

    clean_task = PythonOperator(
        task_id=f"clean_{file.replace('.json', '')}",
        python_callable=clean_data,
        op_kwargs={"file_name": file},
        dag=dag,
    )

    aggregate_task = PythonOperator(
        task_id=f"aggregate_{file.replace('.json', '')}",
        python_callable=aggregate_data,
        op_kwargs={"file_name": file},
        dag=dag,
    )

    mongo_task = PythonOperator(
        task_id=f"push_to_mongo_{file.replace('.json', '')}",
        python_callable=push_to_mongo,
        op_kwargs={"file_name": file},
        dag=dag,
    )

    # DAG Dependencies
    download_task >> clean_task >> aggregate_task >> mongo_task
