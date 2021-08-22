
import logging
from google.cloud import bigquery

def get_max(table_id:str, dataset_id:str, client:bigquery.Client):
    '''
    Query to return the latest block in saved data
    Args:
        table_id(str): table id of saved data
        dataset_id(str): dataset id of saved data
        client(Client): bigQuery client
    
    Return:
        maxblocknumber(int): id of most recent saved block    
    '''
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
