import json
import time
import threading
import requests
import pandas as pd

from datetime import datetime
from azure.eventhub import EventHubConsumerClient

import os
from dotenv import load_dotenv

load_dotenv()




# ---------------------------------------
# Azure Event Hub Configuration
# ---------------------------------------

EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")

EVENT_HUB_NAME = "transaction-stream"


# ---------------------------------------
# Azure Function Configuration
# ---------------------------------------

AZURE_FUNCTION_URL = os.getenv("AZURE_FUNCTION_URL")


# ---------------------------------------
# Consume Transactions
# ---------------------------------------

def consume_transactions(number_of_transactions=5):

    results = []

    stop_event = threading.Event()


    consumer = EventHubConsumerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=EVENT_HUB_NAME
    )


    def on_event(
        partition_context,
        event
    ):

        if event is None:
            return


        if len(results) >= number_of_transactions:
            return


        transaction = json.loads(
            event.body_as_str()
        )


        transaction_id = transaction.get(
            "transaction_id"
        )


        print(
            f"Received transaction ID: {transaction_id}"
        )


        start_time = time.time()


        response = requests.post(
            AZURE_FUNCTION_URL,
            json=transaction,
            timeout=20
        )


        processing_time_ms = (
            time.time() - start_time
        ) * 1000


        prediction = response.json()


        result = {

            "timestamp":
                datetime.now(),

            "transaction_id":
                transaction_id,

            "fraud_probability":
                prediction.get(
                    "fraud_probability",
                    0
                ),

            "status":
                "Fraud"
                if prediction.get(
                    "flagged_suspicious",
                    False
                )
                else "Normal",

            "actual_label":
                transaction.get(
                    "actual_label"
                ),

            "processing_time_ms":
                round(
                    processing_time_ms,
                    2
                )
        }


        results.append(
            result
        )


        print(result)


        partition_context.update_checkpoint(
            event
        )


        # stop receiving after required count

        if len(results) >= number_of_transactions:

            stop_event.set()



    # force receive() to exit

    def close_consumer():

        stop_event.wait()

        print(
            "Closing consumer..."
        )

        consumer.close()



    threading.Thread(
        target=close_consumer,
        daemon=True
    ).start()



    print(
        f"Listening for {number_of_transactions} transactions..."
    )


    with consumer:

        consumer.receive(
            on_event=on_event,
            starting_position="@latest"
        )


    print(
        "Finished receiving transactions"
    )


    df = pd.DataFrame(
        results
    )


    df.to_csv(
        "fraud_predictions.csv",
        index=False
    )


    return df

if __name__ == "__main__":
    consume_transactions(5)