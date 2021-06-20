from datetime import datetime


class BQLogging:
    def __init__(self, client, dataset):
        self.client = client
        self.dataset = dataset

    def insert_log(self, data, status, message):
        sql = f"""
                CREATE TABLE IF NOT EXISTS
                `{self.dataset}.logs`
                (
                    `payloads` STRING,
                    `status` STRING,
                    `message` STRING,
                    `created_at` TIMESTAMP
                );

                INSERT INTO
                    `{self.dataset}.logs`
                    (payloads, status, message, created_at)
                VALUES
                    ("{data}", "{status}", "{message}", "{str(datetime.now())}");
                """
        query_job = self.client.query(sql)
        query_job.result()
        print("LOG added")
