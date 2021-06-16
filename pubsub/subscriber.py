import os
import time
import json
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/tegardp/.keys/academi.json"

PROJECT_ID = "academi-315713"
BQ_DATASET = "stream_staging"
SUBSCRIBER_ID = "test"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIBER_ID)


def callback(message):
    print(f'Received {message}')
    message.ack()
    data = json.loads(message.data)

    insert_operations = [obj for obj in data["activities"]
                         if obj["operation"] == "insert"]
    delete_operations = [obj for obj in data["activities"]
                         if obj["operation"] == "delete"]

    bq_client = bigquery.Client()

    # Insert
    for insert in insert_operations:
        tables = bq_client.list_tables(f"{PROJECT_ID}.{BQ_DATASET}")
        bq_table_name = [table.table_id for table in tables]
        current_table = insert['table']
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{current_table}"

        temp = {}
        for i in range(len(insert["col_names"])):
            temp[insert["col_names"][i]] = insert["col_values"][i]
        row = [temp]

        # Check if table exist in BQ
        if current_table in bq_table_name:
            message = bq_client.insert_rows_json(f"{table_id}", row)
            if message == []:
                print("SUCCESS: New row have been added.")

            # If field not Exist
            elif message[0]['errors'][0]['message'] == "no such field.":
                table = bq_client.get_table(table_id)
                new_column = message[0]['errors'][0]['location']
                col_names = insert["col_names"]
                col_types = insert["col_types"]
                if col_types[col_names.index(new_column)] == "TEXT":
                    new_col_types = "STRING"
                else:
                    new_col_types = col_types[col_names.index(new_column)]
                old_schema = table.schema
                new_schema = old_schema.copy()
                new_schema.append(bigquery.SchemaField(
                    new_column, new_col_types))
                table.schema = new_schema
                table = bq_client.update_table(table, ["schema"])
                time.sleep(10)
                bq_client.insert_rows_json(f"{table_id}", row)
            else:
                print(f"FAILED: While adding new row. {message}")

        # If table not exist create
        else:
            # Create schema
            schema = []
            for i in range(len(insert["col_names"])):
                if insert["col_types"][i] == "TEXT":
                    field = bigquery.SchemaField(
                        insert["col_names"][i], "STRING")
                else:
                    field = bigquery.SchemaField(
                        insert["col_names"][i], insert["col_types"][i])
                schema.append(field)

            # Create table with defined schema
            table = bigquery.Table(table_id, schema=schema)
            table = bq_client.create_table(table)
            print(
                f"SUCCESS: Creating table {table.project}.{table.dataset_id}.{table.table_id}")

            time.sleep(10)
            # Insert data
            message = bq_client.insert_rows_json(
                f"{table.project}.{table.dataset_id}.{table.table_id}", row)
            if message == []:
                print("SUCCESS: New row have been added.")
            else:
                print(f"FAILED: While adding new row. {message}")

    # Delete
    # for delete in delete_operations:
    for delete in delete_operations:
        tables = bq_client.list_tables(f"{PROJECT_ID}.{BQ_DATASET}")
        bq_table_name = [table.table_id for table in tables]
        current_table = delete['table']
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{current_table}"

        condition = []
        for i in range(len(delete["old_value"]["col_names"])):
            if str(delete["old_value"]["col_types"][i]) == "TEXT":
                where = str(delete["old_value"]["col_names"][i]) + \
                    " = '" + str(delete["old_value"]["col_values"][i]) + "'"
                condition.append(where)
            else:
                where = str(delete["old_value"]["col_names"][i]) + \
                    " = " + str(delete["old_value"]["col_values"][i])
                condition.append(where)

        condition = ' AND '.join(condition)
        query = f"DELETE FROM {table_id} WHERE {condition}"

        if current_table in bq_table_name:
            delete_query = bq_client.query(query)
            delete_query.result()
        else:
            print("Table doesn't exist, operation terminated.")


streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.
