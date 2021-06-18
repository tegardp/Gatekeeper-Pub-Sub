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
            elif "no such field" in message[0]['errors'][0]['message']:
                bq_current_table = bq_client.get_table(table_id)
                old_column_names = [schema.name for schema in bq_current_table.schema]
                diff_columns = list(set(insert["col_names"]) - set(old_column_names))
                param_column = ""
                for column in diff_columns:
                    if insert["col_types"][i] == "TEXT":
                        col_types = "STRING"
                    else:
                        col_types= insert['col_types'][insert['col_names'].index(column)]
                    param_column += f"ADD COLUMN {column} {col_types};"

                param_insert = []
                for i in range(len(insert["col_names"])):
                    if insert["col_types"][i] == "TEXT":
                        param_insert.append(f"'{insert['col_values'][i]}'")
                    else:
                        param_insert.append(f"{insert['col_values'][i]}")

                if param_column:
                    sql = f"""
                    ALTER TABLE `{table_id}`
                    {param_column}
                    """
                    query_job = bq_client.query(sql)
                    query_job.result()
                    print(
                        f"SUCCESS: Add new column to {table_id}")

                sql = f"""
                INSERT INTO `{table_id}`
                ({','.join(insert['col_names'])})
                VALUES
                ({','.join(param_insert)})
                """
                
                query_job = bq_client.query(sql)
                query_job.result()
                print(
                    f"SUCCESS: Add new row to {table_id}")
        # If table not exist create
        else:
            param = ""
            for i in range(len(insert["col_names"])):
                if insert["col_types"][i] == "TEXT":
                    col_types = "STRING"
                else:
                    col_types = insert["col_types"][i]
                param += f"`{insert['col_names'][i]}` {col_types},"

            sql = f"""
            CREATE TABLE IF NOT EXISTS
            `{table_id}`
            ({param})
            """
            
            try:
                query_job = bq_client.query(sql)
                query_job.result()
                print(
                    f"SUCCESS: Creating table {table_id}")
                    # Insert data
                message = bq_client.insert_rows_json(
                    f"{table_id}", row)
                if message == []:
                    print("SUCCESS: New row have been added.")
                else:
                    print(f"FAILED: While adding new row. {message}")
            except:
                print(
                    f"FAILED: Creating table {table_id}")

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

        try:
            delete_query = bq_client.query(query)
            delete_query.result()
            print("rows deleted.")
        except:
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
