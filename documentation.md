### Progress So Far  

We have successfully loaded data onto GCS and MongoDB and verified that all team members can connect without issues.  

The flow is using GCS as the upstream source of truth, with Airflow as the orchestrator to:  

- Pull raw data from GCS  
- Clean and preprocess it  
- Run aggregations  
- Push the refined datasets to MongoDB
- Access MongoDB from python directly and create models for prediction.

The data structure is finalized, with clear definitions on which predictors to keep for modeling and which aggregations provide the best insights.  

These decisions are documented in `mongo_doc_restructure.txt` and `queries.txt`, both of which have been tested and confirm that we are getting the structure we want in MongoDB. Attached is also the master DAG visual to showcase the overall structure of the pipeline (`gcs_to_mongo_pipeline.png`).  

Additionally, we have rifined a subset of the ataframes to be in an easier to work with format. The datasets affected are **CO2, Global Temperature, and Ice Sheets**.  

We have additionally included 4 notebooks that create models for predicting future values from historical values.

- `lpi_model.ipynb`
- `sea_level.ipynb`
- `CO2_level.ipynb`
- `globaltemp_pred.ipynb`

### Fixed from last check-in  

- Fixed issues with DAG, now able to run as expected. DAG is able to pull data from GCS when being executed by AirFlow.
- Airflow → GCS authentication and DAG can access the bucket.
- All team members have access and can run the DAG without hitting permission issues.  
- Full cloud-based DAG working, replacing local sourcing with automated GCS retrieval.  

