# Sol_ETL
SolanaBeach API to BigQuery ETL

### Instructions
1) Create BigQuery Table
2) Create a Google Cloud Composer Environment
3) Use [dag](https://github.com/RLP2/sol_etl/blob/main/dags/dag.py) to run:

      a) [max_block_query](https://github.com/RLP2/sol_etl/blob/main/src/max_block_query.py) (which passes on the max block of saved data) <br> 
      b) [extract](https://github.com/RLP2/sol_etl/blob/main/src/extract.py) (which pulls data from solanabeach api) <br>
      c) [load](https://github.com/RLP2/sol_etl/blob/main/src/load.py) (which loads the pulled data into the saved bigquery table) <br>
 
