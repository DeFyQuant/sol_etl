# Sol_ETL
SolanaBeach API to BigQuery ETL

### Instructions
1) Create BigQuery Table
2) Create a Google Cloud Composer Environment
3) Use dags/dag.py to run:

      a) max_block_query.py (which passes on the max block of saved data)
      b) extract.py (which pulls data from solanabeach api)
      c) load.py (which loads the pulled data into the saved bigquery table)
