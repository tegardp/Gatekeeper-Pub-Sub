import os
import json
from logger import BQLogging
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

    logging = BQLogging(bq_client, f"{PROJECT_ID}.{BQ_DATASET}")
    bq_operation = BQOperations(bq_client)

    tables = bq_client.list_tables(f"{PROJECT_ID}.{BQ_DATASET}")
    bq_table_name = [table.table_id for table in tables]

    # Insert
    for operation in data["activities"]:
        current_table = operation['table']
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{current_table}"
        # temp = {}
        # for i in range(len(insert["col_names"])):
        #     temp[insert["col_names"][i]] = insert["col_values"][i]
        # row = [temp]
        tables = bq_client.list_tables(f"{PROJECT_ID}.{BQ_DATASET}")
        bq_table_name = [table.table_id for table in tables]
        # # Check if table exist in BQ
        if current_table in bq_table_name:
            bq_operation.insert_row(operation, table_id)
        #     message = bq_client.insert_rows_json(f"{table_id}", row)
        #     if message == []:
        #         msg = f"SUCCESS: New row have been added to {table_id}"
        #         logging.insert_log(insert, "Success", msg)
        #         print(msg)

        #     # If field not Exist
        #     elif "no such field" in message[0]['errors'][0]['message']:
        #         bq_current_table = bq_client.get_table(table_id)
        #         old_column_names = [
        #             schema.name for schema in bq_current_table.schema]
        #         diff_columns = list(
        #             set(insert["col_names"]) - set(old_column_names))
        #         param_column = ""
        #         for column in diff_columns:
        #             if insert["col_types"][i] == "TEXT":
        #                 col_types = "STRING"
        #             else:
        #                 col_types = insert['col_types'][insert['col_names'].index(
        #                     column)]
        #             param_column += f"ADD COLUMN {column} {col_types};"

        #         param_insert = []
        #         for i in range(len(insert["col_names"])):
        #             if insert["col_types"][i] == "TEXT":
        #                 param_insert.append(f"'{insert['col_values'][i]}'")
        #             else:
        #                 param_insert.append(f"{insert['col_values'][i]}")

        #         if param_column:
        #             sql = f"""
        #             ALTER TABLE `{table_id}`
        #             {param_column}
        #             """
        #             query_job = bq_client.query(sql)
        #             query_job.result()
        #             print(
        #                 f"SUCCESS: Add new column to {table_id}")

        #         sql = f"""
        #         INSERT INTO `{table_id}`
        #         ({','.join(insert['col_names'])})
        #         VALUES
        #         ({','.join(param_insert)})
        #         """

        #         query_job = bq_client.query(sql)
        #         query_job.result()

        #         msg = f"SUCCESS: New column and row have been added to {table_id}"
        #         logging.insert_log(insert, "Success", msg)
        #         print(msg)

        # If table not exist create
        else:
            bq_operation.create_table(operation, table_id)
            # param = ""
            # for i in range(len(insert["col_names"])):
            #     if insert["col_types"][i] == "TEXT":
            #         col_types = "STRING"
            #     else:
            #         col_types = insert["col_types"][i]
            #     param += f"`{insert['col_names'][i]}` {col_types},"

            # sql = f"""
            # CREATE TABLE IF NOT EXISTS
            # `{table_id}`
            # ({param})
            # """

            # try:
            #     query_job = bq_client.query(sql)
            #     query_job.result()

            #     print(
            #         f"SUCCESS: Creating table {table_id}")
            #     # Insert data
            #     message = bq_client.insert_rows_json(
            #         f"{table_id}", row)
            #     if message == []:
            #         msg = f"SUCCESS: New row have been added. {table_id}"
            #         logging.insert_log(insert, "Success", msg)
            #         print(msg)
            #     else:
            #         msg = f"FAILED: While adding new row. {message}"
            #         logging.insert_log(insert, "Failed", msg)
            #         print(msg)
            # except:
            #     msg = f"FAILED: Creating table {table_id}"
            #     logging.insert_log(insert, "Failed", msg)
            #     print(msg)

        if "delete" in operation["operation"]:
            current_table = operation['table']
            table_id = f"{PROJECT_ID}.{BQ_DATASET}.{current_table}"
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
