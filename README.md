# Sample_Great_Expectaions

This repo contains the code for connecting great expectations library to in memory Spark DataFrame and run the validation suite.

#### Data Source:
great_expectations/great_expectations.yml -> `my_spark_dataframe`

#### Validation Suite:
great_expectations/metadata/expectations/taxi_suite/demo.json
#### Checkpoint:
great_expectations/metadata/checkpoints/placeholder_checkpoint.yml

The above checkpoint is just placeholder for batch information, which we will be providing in the py file.

#### Prerequisites:
- install python, pyspark, java
- install great expectations
  - pip install great_expectations
- clone the repo
  - git clone https://github.com/anuj1207/sample_great_expectations.git
- run main.py
  - python3 main.py

#### Execution:
- Reads the data from `sample-data` and creates a DF.
- Pass the DF to the BatchRequest Created for Great_expectations.
- Pass BatchRequest and checkpoint to context
- Validate the data through context config
- Open the data doc in browser for report (file:///`<path-to-this repo>`/great_expectations/metadata/uncommitted/data_docs/local_site/index.html)
