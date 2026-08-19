import requests
import time
import pandas as pd

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------
# Azure Function Endpoint
# ---------------------------------------------------------
AZURE_FUNCTION_URL = os.getenv("AZURE_FUNCTION_URL")



# ---------------------------------------------------------
# Cloud Transaction Stream Simulator
# ---------------------------------------------------------

def simulate_cloud_stream(
        X_stream,
        y_stream,
        df_raw,
        delay_seconds=0.5,
        n_transactions=200,
        threshold=0.5
):

    """
    Replay transactions one by one.

    X_stream:
        Contains:
        Amount_scaled
        Time_scaled
        V1 ... V28

    df_raw:
        Original dataframe containing:
        Amount
        Time

    Sends each transaction to Azure Function.

    Returns:
        DataFrame of prediction logs
    """


    stream_X = (
        X_stream
        .iloc[:n_transactions]
        .reset_index()
    )


    stream_y = (
        y_stream
        .iloc[:n_transactions]
        .reset_index(drop=True)
    )


    logs = []


    for i in range(len(stream_X)):


        row = stream_X.iloc[i]


        original_index = row["index"]


        # ----------------------------------
        # Recover original Amount and Time
        # ----------------------------------

        raw_values = df_raw.loc[
            original_index,
            [
                "Amount",
                "Time"
            ]
        ]


        # ----------------------------------
        # Create Azure payload
        # ----------------------------------

        payload = (
            row
            .drop(
                [
                    "index",
                    "Amount_scaled",
                    "Time_scaled"
                ],
                errors="ignore"
            )
            .to_dict()
        )


        payload["Amount"] = float(
            raw_values["Amount"]
        )


        payload["Time"] = float(
            raw_values["Time"]
        )


        # ----------------------------------
        # Send transaction to Azure Function
        # ----------------------------------

        start_time = time.time()


        try:

            response = requests.post(
                AZURE_FUNCTION_URL,
                json=payload,
                timeout=30
            )


            elapsed_ms = (
                time.time() - start_time
            ) * 1000


            result = response.json()


            probability = result.get(
                "fraud_probability",
                None
            )


            flagged = result.get(
                "flagged_suspicious",
                False
            )


        except Exception as e:


            probability = None

            flagged = False

            elapsed_ms = 0


            print(
                "Azure error:",
                e
            )


        # ----------------------------------
        # Store result
        # ----------------------------------

        logs.append(
            {

                "transaction_id": i,

                "fraud_probability":
                    probability,


                "flagged_suspicious":
                    flagged,


                "actual_label":
                    int(stream_y.iloc[i]),


                "processing_time_ms":
                    round(
                        elapsed_ms,
                        3
                    )

            }
        )


        if flagged:

            print(
                f"[{i:04d}] ALERT 🚨 "
                f"Fraud probability = {probability:.4f}"
            )


        time.sleep(
            delay_seconds
        )


    return pd.DataFrame(logs)