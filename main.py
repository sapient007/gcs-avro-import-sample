import functions_framework
import os
from google.cloud import storage
from google.cloud import bigquery


#Global Sample Variables
avro_import_bucket = "avro-import" # Bucket where events in Avro format are exported
event1_bucket = "bq-avro-event1-data" # Bucket where event type 1 (i.e redirect) occurs
event2_bucket = "bq-avro-event1-data"  # Bucket where event type 2 (i.e login error) occurs
bq_table_id_event1 = "bq-avro-import.avro_import.imported_data_event1" #BQ Project.Dataset.TableName for event type 1
bq_table_id_event2 = "bq-avro-import.avro_import.imported_data_event2" #BQ Project.Dataset.TableName for event type 2

# # Triggered by a change in a storage bucket
@functions_framework.cloud_event
def load_events(cloud_event):
    """Imort all AVRO files to BQ and archive all the blobs"""
    storage_client = storage.Client()

    source_bucket = storage_client.bucket(avro_import_bucket)
    blobs = storage_client.list_blobs(avro_import_bucket)
    destination_bucket_event1 = storage_client.bucket(event1_bucket)
    destination_bucket_event2 = storage_client.bucket(event2_bucket)

    # Note: The call returns a response only when the iterator is consumed.
    for blob in blobs:

        # Optional: set a generation-match precondition to avoid potential race conditions
        # and data corruptions. The request to copy is aborted if the object's
        # generation number does not match your precondition. For a destination
        # object that does not yet exist, set the if_generation_match precondition to 0.
        # If the destination object already exists in your bucket, set instead a
        # generation-match precondition using its generation number.
        # There is also an `if_source_generation_match` parameter, which is not used in this example.
        destination_generation_match_precondition = 0


        #strip folder name from file string
        destination_blob_name = os.path.basename(blob.name)
        event_type = os.path.dirname(blob.name)

        source_blob = source_bucket.blob(blob.name)

        #import data based on event type 
        if event_type == "Event1_String":
            #import data into BQ 
            load_data_to_bq_event1(blob.name)

            #move file to archived folder for event_1
            blob_copy = source_bucket.copy_blob(
                source_blob, event1_bucket, destination_blob_name, if_generation_match=destination_generation_match_precondition,
            )
            print("copyed " + blob.name + " to " + event1_bucket + " as " + destination_blob_name )

        elif event_type == "Event2_String":
            #import data into BQ 
            load_data_to_bq_event2(blob.name)

            #move file to archived folder for event_1
            blob_copy = source_bucket.copy_blob(
                source_blob, event2_bucket, destination_blob_name, if_generation_match=destination_generation_match_precondition,
            )
            print("copyed " + blob.name + " to " + event2_bucket + " as " + destination_blob_name )
        else:
            #do nothing event type not understood
            pass

        #delete source blob since it has been copied to archive folder
        source_bucket.delete_blob(blob.name)
            

def load_data_to_bq_event1():
    # reference https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#appending_to_or_overwriting_a_table_with_avro_data
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("post_abbr", "STRING"),
        ],
    )

    body = io.BytesIO(b"Washington,WA")
    client.load_table_from_file(body, table_id, job_config=job_config).result()
    previous_rows = client.get_table(table_id).num_rows
    assert previous_rows > 0

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.AVRO,
    )

    uri = "gs://cloud-samples-data/bigquery/us-states/us-states.avro"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

def load_data_to_bq_event2():
    # reference https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#appending_to_or_overwriting_a_table_with_avro_data
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("post_abbr", "STRING"),
        ],
    )

    body = io.BytesIO(b"Washington,WA")
    client.load_table_from_file(body, table_id, job_config=job_config).result()
    previous_rows = client.get_table(table_id).num_rows
    assert previous_rows > 0

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.AVRO,
    )

    uri = "gs://cloud-samples-data/bigquery/us-states/us-states.avro"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))
