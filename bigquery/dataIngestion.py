import os
from google.oauth2 import service_account
from google.cloud import bigquery
from pathlib import Path
from configFileReader import credentials,project_id,dataset_id,table_id,result_file_path

credentials = service_account.Credentials.from_service_account_file(
    credentials)

# Construct a BigQuery client object.
client = bigquery.Client(credentials=credentials, project=project_id)


# CREATE dataset
def create_dataset(datasetId):
    dataset_ref = bigquery.DatasetReference.from_string(datasetId, default_project=client.project)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    try:
        dataset = client.create_dataset(dataset)
        print("Dataset is created {}.{} successfully ".format(client.project, dataset.dataset_id))
    except:
        print("Dataset is already exists.")


# load local file into bigquery table
def write_data_into_bigquery(dataFileFolder, tableId: str):
    global csvFile
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="row_created_timestamp",
        )
    )
    for file in os.listdir(dataFileFolder):
        if file.endswith('.csv'):
            print('Processing file: {0}'.format(file))
            csvFile = dataFileFolder.joinpath(file)
    with open(csvFile, 'rb') as source_file:
        job = client.load_table_from_file(source_file, tableId, job_config=job_config)
    job.result()
    table = client.get_table(tableId)
    print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), tableId))


create_dataset(datasetId=dataset_id)
write_data_into_bigquery(Path(result_file_path), table_id)
