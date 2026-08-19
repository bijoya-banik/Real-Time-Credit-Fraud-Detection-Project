"""
Azure Function — Real-Time Credit Card Fraud Scoring Endpoint
----------------------------------------------------------------
HTTP-triggered function that loads the trained model and feature scalers.

Local development:
    Loads model/scalers from ./models folder

Azure deployment:
    Loads model/scalers from Azure Blob Storage

Request: 
    Sends raw Amount and Time values.
    Function performs scaling internally.

Expected input:
{
    "V1": -1.35,
    ...
    "V28": -0.02,
    "Amount": 149.62,
    "Time": 406.0
}
"""

import json
import logging
import os
import tempfile

import azure.functions as func
import joblib
import numpy as np
from azure.storage.blob import BlobServiceClient


app = func.FunctionApp(
    http_auth_level=func.AuthLevel.FUNCTION
)


# Cached objects (warm execution reuse)
_model = None
_amount_scaler = None
_time_scaler = None


# ---------------------------------------------------------
# Local loading (development)
# ---------------------------------------------------------

def _load_local_file(filename: str):
    """
    Load model/scaler from local models folder.
    """

    path = os.path.join(
        os.path.dirname(__file__),
        "models",
        filename
    )

    logging.info(f"Loading local file: {path}")

    return joblib.load(path)



# ---------------------------------------------------------
# Azure Blob loading (production)
# ---------------------------------------------------------

def _load_from_blob(blob_name: str):
    """
    Download blob and load with joblib.
    """

    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]

    container = os.environ.get(
        "MODEL_CONTAINER",
        "fraud-models"
    )

    blob_service = BlobServiceClient.from_connection_string(
        conn_str
    )

    blob_client = blob_service.get_blob_client(
        container=container,
        blob=blob_name
    )


    with tempfile.NamedTemporaryFile(
        delete=False
    ) as tmp:

        tmp.write(
            blob_client.download_blob().readall()
        )

        tmp_path = tmp.name


    return joblib.load(tmp_path)



# ---------------------------------------------------------
# Load model and scalers once
# ---------------------------------------------------------

def _get_model_and_scalers():

    global _model
    global _amount_scaler
    global _time_scaler


    if _model is None:

        # Azure environment
        if "AZURE_STORAGE_CONNECTION_STRING" in os.environ:

            logging.info(
                "Loading model from Azure Blob Storage"
            )


            _model = _load_from_blob(
                os.environ.get(
                    "MODEL_BLOB_NAME",
                    "best_fraud_model.pkl"
                )
            )


            _amount_scaler = _load_from_blob(
                os.environ.get(
                    "AMOUNT_SCALER_BLOB_NAME",
                    "amount_scaler.pkl"
                )
            )


            _time_scaler = _load_from_blob(
                os.environ.get(
                    "TIME_SCALER_BLOB_NAME",
                    "time_scaler.pkl"
                )
            )


        # Local development
        else:

            logging.info(
                "Loading model from local models folder"
            )


            _model = _load_local_file(
                "best_fraud_model.pkl"
            )


            _amount_scaler = _load_local_file(
                "amount_scaler.pkl"
            )


            _time_scaler = _load_local_file(
                "time_scaler.pkl"
            )


    return (
        _model,
        _amount_scaler,
        _time_scaler
    )



# ---------------------------------------------------------
# HTTP Trigger
# ---------------------------------------------------------

@app.route(
    route="score",
    methods=["POST"]
)
def score(req: func.HttpRequest) -> func.HttpResponse:

    """
    Score one credit card transaction.

    Input:
    - V1 to V28
    - Amount
    - Time

    Output:
    - fraud probability
    - suspicious flag
    """

    try:

        body = req.get_json()


    except ValueError:

        return func.HttpResponse(
            json.dumps(
                {
                    "error": "Request body must be JSON"
                }
            ),
            status_code=400,
            mimetype="application/json"
        )


    try:

        model, amount_scaler, time_scaler = (
            _get_model_and_scalers()
        )


        # ------------------------------
        # Scale Amount and Time
        # ------------------------------

        scaled_amount = float(
            amount_scaler.transform(
                [[body["Amount"]]]
            )[0][0]
        )


        scaled_time = float(
            time_scaler.transform(
                [[body["Time"]]]
            )[0][0]
        )


        # ------------------------------
        # Prepare model input
        # ------------------------------

        values = {
            k: v
            for k, v in body.items()
            if k not in ("Amount", "Time")
        }


        values["Amount_scaled"] = scaled_amount
        values["Time_scaled"] = scaled_time



        # Keep same feature order as training

        if hasattr(
            model,
            "feature_names_in_"
        ):

            feature_order = (
                model.feature_names_in_
            )

        else:

            feature_order = sorted(
                values.keys()
            )


        row = np.array(
            [
                [
                    values[col]
                    for col in feature_order
                ]
            ]
        )


        probability = float(
            model.predict_proba(row)[0][1]
        )


        flagged = probability >= 0.5



        response = {

            "fraud_probability": probability,

            "flagged_suspicious": flagged

        }


        return func.HttpResponse(
            json.dumps(response),
            status_code=200,
            mimetype="application/json"
        )


    except KeyError as e:

        return func.HttpResponse(
            json.dumps(
                {
                    "error": f"Missing feature: {e}"
                }
            ),
            status_code=400,
            mimetype="application/json"
        )


    except Exception as e:

        logging.exception(
            "Scoring failed"
        )


        return func.HttpResponse(
            json.dumps(
                {
                    "error": "Internal scoring error",
                    "detail": str(e)
                }
            ),
            status_code=500,
            mimetype="application/json"
        )