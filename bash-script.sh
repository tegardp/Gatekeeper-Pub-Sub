 #!/bin/bash

MY_TOPIC="academi"
TEST_SUB="test"
BQ_SUB="heimdall"
PG_SUB="thor"

# Create topics
gcloud pubsub topics create $MY_TOPIC

# Create subscriptions
gcloud pubsub subscriptions create $TEST_SUB --topic $MY_TOPIC

# Run Gatekeeper API
# pip install virtualenv
# python venv myenv
# python install -r requirements.txt
python app/main.py