
import logging
from google.cloud import bigquery

def get_max(table_id:str, dataset_id:str, client:bigquery.Client):

    dataset = client.create_dataset(dataset_id)

    query = f"""
        SELECT max(blocknumber)
        FROM `{table_id}`

    """
    query_job = client.query(
        query,
        location="US",
    ) 

    return int(m.to_dataframe().values[0])
