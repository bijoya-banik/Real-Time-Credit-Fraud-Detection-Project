import os
import json
import pandas as pd

from azure.eventhub import (
    EventHubProducerClient,
    EventData
)

import os
from dotenv import load_dotenv


# ---------------------------------------
# Azure Event Hub Configuration
# ---------------------------------------

load_dotenv()

EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")


EVENT_HUB_NAME = "transaction-stream"



# ---------------------------------------
# Dataset Path
# ---------------------------------------

BASE_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

DATASET_PATH = os.path.join(
    BASE_DIR,
    "data",
    "creditcard.csv"
)



# ---------------------------------------
# Send Transactions
# ---------------------------------------

def send_transactions(
    number_of_transactions=5
):
    """
    Send random credit card transactions
    to Azure Event Hub.

    Returns:
        List of transaction IDs sent
    """


    # Load dataset

    df = pd.read_csv(
        DATASET_PATH
    )


    # Random sampling

    sample_df = df.sample(
        n=number_of_transactions,
        random_state=None
    )


    sent_ids = []


    # Create producer

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME
    )


    with producer:


        for index, row in sample_df.iterrows():


            transaction = row.to_dict()


            # Add unique ID

            transaction_id = int(index)

            transaction["transaction_id"] = (
                transaction_id
            )


            # Save actual label for evaluation

            if "Class" in transaction:

                transaction["actual_label"] = int(
                    transaction["Class"]
                )


            # Convert values for JSON

            clean_transaction = {}

            for key, value in transaction.items():


                if key in [
                    "transaction_id",
                    "actual_label"
                ]:

                    clean_transaction[key] = int(value)


                else:

                    clean_transaction[key] = float(value)



            # Create event

            event = EventData(
                json.dumps(
                    clean_transaction
                )
            )


            batch = producer.create_batch()


            batch.add(
                event
            )


            producer.send_batch(
                batch
            )


            sent_ids.append(
                transaction_id
            )


            print(
                f"Sent transaction ID: {transaction_id}"
            )


    print(
        f"\n✅ {len(sent_ids)} random transactions sent successfully"
    )


    return sent_ids



# ---------------------------------------
# Direct Test
# ---------------------------------------

if __name__ == "__main__":

    send_transactions(5)