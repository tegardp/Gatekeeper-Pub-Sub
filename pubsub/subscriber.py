import os
import json
from BQOperations import BQOperations
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../key.json"

PROJECT_ID = "academi-315713"
BQ_DATASET = "stream_staging"
SUBSCRIBER_ID = "test"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIBER_ID)


def callback(message):
    print(f'Received {message}')
    message.ack()
    data = json.loads(message.data)

    bq_client = bigquery.Client()
    bq_operation = BQOperations(bq_client)

    tables = bq_client.list_tables(f"{PROJECT_ID}.{BQ_DATASET}")
    bq_table_name = [table.table_id for table in tables]

    # Insert
    for operation in data["activities"]:
        current_table = operation['table']
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{current_table}"
        tables = bq_client.list_tables(f"{PROJECT_ID}.{BQ_DATASET}")
        bq_table_name = [table.table_id for table in tables]

        if "insert" in operation["operation"]:
            # # Check if table exist in BQ
            if current_table in bq_table_name:
                bq_operation.insert_row(operation, table_id)

            # If table not exist create
            else:
                bq_operation.create_table(operation, table_id)

        if "delete" in operation["operation"]:
            bq_operation.delete_row(operation, table_id)


streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.
