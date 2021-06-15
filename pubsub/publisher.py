import os
from google.cloud import pubsub_v1

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/tegardp/.keys/academi.json"

PROJECT_ID = "academi-315713"
TOPIC_ID = "academi"

publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


for n in range(1, 10):
    data = "Message number {}".format(n)
    # Data must be a bytestring
    data = data.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data)
    print(future.result())

print(f"Published messages to {topic_path}.")
