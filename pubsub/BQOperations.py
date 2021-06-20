from google.cloud.bigquery import table
from logger import BQLogging

PROJECT_ID = "academi-315713"
BQ_DATASET = "stream_staging"


class BQOperations:
    def __init__(self, client):
        self.client = client
        self.logging = BQLogging(self.client, f"{PROJECT_ID}.{BQ_DATASET}")

    def create_table(self, operation, table_id):
        self.logging = BQLogging(self.client, f"{PROJECT_ID}.{BQ_DATASET}")

        param = ""
        for i in range(len(operation["col_names"])):
            if operation["col_types"][i] == "TEXT":
                col_types = "STRING"
            else:
                col_types = operation["col_types"][i]
            param += f"`{operation['col_names'][i]}` {col_types},"

        sql = f"""
        CREATE TABLE IF NOT EXISTS
        `{table_id}`
        ({param})
        """

        try:
            query_job = self.client.query(sql)
            query_job.result()

            print(
                f"SUCCESS: Creating table {table_id}")
            # Insert data
            self.insert_row(operation, table_id)
        except:
            msg = f"FAILED: Creating table {table_id}"
            self.logging.insert_log(operation, "Failed", msg)
            print(msg)

    def insert_row(self, operation, table_id):
        temp = {}

        for i in range(len(operation["col_names"])):
            temp[operation["col_names"][i]] = operation["col_values"][i]
        row = [temp]

        message = self.client.insert_rows_json(f"{table_id}", row)
        if message == []:
            msg = f"SUCCESS: New row have been added to {table_id}"
            self.logging.insert_log(operation, "Success", msg)
            print(msg)

        # If field not Exist
        elif "no such field" in message[0]['errors'][0]['message']:
            bq_current_table = self.client.get_table(table_id)
            old_column_names = [
                schema.name for schema in bq_current_table.schema]
            diff_columns = list(
                set(operation["col_names"]) - set(old_column_names))
            param_column = ""
            for column in diff_columns:
                if operation["col_types"][i] == "TEXT":
                    col_types = "STRING"
                else:
                    col_types = operation['col_types'][operation['col_names'].index(
                        column)]
                param_column += f"ADD COLUMN {column} {col_types};"

            param_insert = []
            for i in range(len(operation["col_names"])):
                if operation["col_types"][i] == "TEXT":
                    param_insert.append(f"'{operation['col_values'][i]}'")
                else:
                    param_insert.append(f"{operation['col_values'][i]}")

            if param_column:
                sql = f"""
                ALTER TABLE `{table_id}`
                {param_column}
                """
                query_job = self.client.query(sql)
                query_job.result()
                print(
                    f"SUCCESS: Add new column to {table_id}")

            sql = f"""
            INSERT INTO `{table_id}`
            ({','.join(operation['col_names'])})
            VALUES
            ({','.join(param_insert)})
            """

            query_job = self.client.query(sql)
            query_job.result()

            msg = f"SUCCESS: New column and row have been added to {table_id}"
            self.logging.insert_log(operation, "Success", msg)
            print(msg)
        else:
            msg = f"FAILED: While adding new row. {message}"
            self.logging.insert_log(operation, "Failed", msg)
            print(msg)

    def delete_row(self, operation, table_id):
        bq_tables = self.client.list_tables(f"{PROJECT_ID}.{BQ_DATASET}")
        bq_table_name = [table.table_id for table in bq_tables]

        if operation['table'] in bq_table_name:
            condition = []
            for i in range(len(operation["old_value"]["col_names"])):
                if str(operation["old_value"]["col_types"][i]) == "TEXT":
                    where = str(operation["old_value"]["col_names"][i]) + \
                        " = '" + str(operation["old_value"]
                                     ["col_values"][i]) + "'"
                    condition.append(where)
                else:
                    where = str(operation["old_value"]["col_names"][i]) + \
                        " = " + str(operation["old_value"]["col_values"][i])
                    condition.append(where)

            condition = ' AND '.join(condition)
            query = f"DELETE FROM {table_id} WHERE {condition}"

            delete_query = self.client.query(query)
            delete_query.result()

            msg = f"SUCCESS: Rows deleted from {table_id}"
            self.logging.insert_log(operation, "Success", msg)
            print(msg)
        else:
            msg = f"FAILED: {table_id} doesn't exist, operation terminated."
            self.logging.insert_log(operation, "Failed", msg)
            print(msg)
