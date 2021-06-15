import os
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/tegardp/.keys/academi.json"

PROJECT_ID="academi-315713"
SUBSCRIBER_ID="test"

subscriber = pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIBER_ID)

def callback(message):
    print(f'Received {message}')
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.