
import logging
from google.cloud import bigquery

def load_temp_to_perm(table_id:str, dataset_id:str, source_filename:str, client:bigquery.Client):

    dataset = client.create_dataset(dataset_id)

    table_ref = dataset.table('block_from_local_file')
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    with open(source_filename, 'rb') as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location='US',
            job_config=job_config)

    # wait for table load to complete
    job.result()
    
    #logging
    if job.errors:
        logging.errors(f'job failed: {job.errors}')

    logging.info(f'Loaded {job.output_rows} rows into {dataset_id}:{table_ref.path}.')



    query = f"""
        INSERT INTO `{table_id}`
        SELECT *
        FROM `alien-sol-322920.temp.block_from_local_file`

    """
    query_job = client.query(
        query,
        location="US",
    ) 

    df = query_job.to_dataframe()


    # Delete the dataset and its contents
    dataset = client.get_dataset(client.dataset(dataset_id))
    client.delete_dataset(dataset, delete_contents=True)
    logging.info('Deleted dataset: {dataset.path}')