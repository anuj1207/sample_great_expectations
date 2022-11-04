# Sample_Great_Expectaions

This repo contains the code for connecting great expectations library to in memory Spark DataFrame and run the validation suite.

#### Prerequisites:
- install python, pyspark, java
- install great expectations
  - pip install great_expectations
- clone the repo
  - git clone https://github.com/anuj1207/sample_great_expectations.git


### 1. Using Spark DataFrame great expectations configuration YML and expectation JSON

#### Data Source:
great_expectations/great_expectations.yml -> `my_spark_dataframe`

#### Validation Suite:
great_expectations/metadata/expectations/taxi_suite/demo.json
#### Checkpoint:
great_expectations/metadata/checkpoints/placeholder_checkpoint.yml

The above checkpoint is just placeholder for batch information, which we will be providing in the py file.

#### Execution:
- Run API validation file
  - python3 batch_request_validation.py


#### Flow:
- Reads the data from `sample-data` and creates a Spark DF.
- Pass the DF to the BatchRequest Created for Great_expectations.
- Pass BatchRequest and checkpoint to context
- Validate the data through context config
- Open the data doc in browser for report (file:///`<path-to-this repo>`/great_expectations/metadata/uncommitted/data_docs/local_site/index.html)


### 2. Using Spark DataFrame great expectations python API

#### Execution:
- Run API validation file
  - python3 df_api_validation.py

#### Flow:
- Reads the data from `sample-data` and creates a Spark DF.
- Creates a Great Expectation DF (a wrapper over Spark DF)
- Calls the expectations using the API methods over the Great Expectation DF
- Prints the output in console

### 3. Using Spark DataFrame great expectations python API with expectattion JSON files

#### Execution:
- Run API validation file
  - python3 df_api_json_validation.py

#### Flow:
- Reads the data from `sample-data` and creates a Spark DF.
- Creates a Great Expectation DF (a wrapper over Spark DF)
- Calls the validation API method with the provided expectations JSON file path over the Great Expectation DF
- Prints the output in console
s