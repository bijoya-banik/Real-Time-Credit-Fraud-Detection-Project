
import streamlit as st
import pandas as pd
import sys
import os
import threading
import time
import matplotlib.pyplot as plt 


# =========================================================
# Import Project Files
# =========================================================

PROJECT_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        ".."
    )
)

sys.path.append(PROJECT_DIR)

from event_producer import send_transactions
from event_consumer import consume_transactions


# =========================================================
# Page Configuration
# =========================================================

st.set_page_config(
    page_title="Real-Time Credit Card Fraud Detection Dashboard",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded"
)


# =========================================================
# CSS - Presentation / Screenshot Friendly
# =========================================================

st.markdown(
    """
    <style>

    /* =====================================================
       Main Page
       ===================================================== */

    .block-container {
        padding-top: 0.5rem !important;
        padding-bottom: 0.5rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
    }


    /* =====================================================
       Remove Streamlit Top Header
       ===================================================== */

    header[data-testid="stHeader"] {
        display: none;
    }


    div[data-testid="stAppViewContainer"] > .main {
        padding-top: 0rem;
    }


    /* =====================================================
       Main Dashboard Title
       ===================================================== */

    h1 {
        font-size: 2rem !important;
        font-weight: 700 !important;
        margin-top: 0 !important;
        margin-bottom: 0.15rem !important;
    }


    /* =====================================================
       Section Titles
       ===================================================== */

    h2,
    h3 {
        font-size: 1.15rem !important;
        font-weight: 700 !important;
        margin-top: 0.15rem !important;
        margin-bottom: 0.3rem !important;
    }


    /* =====================================================
       Compact Vertical Spacing
       ===================================================== */

    div[data-testid="stVerticalBlock"] {
        gap: 0.35rem;
    }


    hr {
        margin: 0.3rem 0 !important;
    }


    /* =====================================================
       Summary Metrics
       ===================================================== */

    div[data-testid="stMetric"] {
        padding: 0.3rem 0.5rem;
    }


    div[data-testid="stMetricLabel"] {
        font-size: 0.85rem !important;
        font-weight: 600 !important;
    }


    div[data-testid="stMetricValue"] {
        font-size: 1.55rem !important;
        font-weight: 700 !important;
    }


    /* =====================================================
       Dataframe - Large Presentation Text
       ===================================================== */

    div[data-testid="stDataFrame"] {
        font-size: 16px !important;
    }


    div[data-testid="stDataFrame"] td {
        font-size: 16px !important;
        font-weight: 500 !important;
    }


    div[data-testid="stDataFrame"] th {
        font-size: 16px !important;
        font-weight: 700 !important;
    }


    /* =====================================================
       Download Button
       ===================================================== */

    div[data-testid="stDownloadButton"] button {
        font-size: 14px !important;
        font-weight: 600 !important;
        padding: 0.45rem 0.7rem !important;
    }


    /* =====================================================
       Sidebar
       ===================================================== */

    section[data-testid="stSidebar"] {
        padding-top: 0.5rem;
    }


    /* =====================================================
       Sidebar Success Messages
       ===================================================== */

    section[data-testid="stSidebar"] div[data-testid="stAlert"] {
        font-size: 0.9rem !important;
    }

    /* =====================================================
   DataFrame Header - Black & Bold
   ===================================================== */

    div[data-testid="stDataFrame"] th {
        background-color: #000000 !important;
        color: #ffffff !important;
        font-size: 16px !important;
        font-weight: 700 !important;
    }

    div[data-testid="stDataFrame"] th div {
        color: #ffffff !important;
        font-weight: 700 !important;
    }


    </style>
    """,
    unsafe_allow_html=True
)


# =========================================================
# Header
# =========================================================

st.title(
    "💳 Real-Time Credit Card Fraud Detection Dashboard"
)


# =========================================================
# Sidebar - Simulation Control
# =========================================================

st.sidebar.header(
    "Simulation Control"
)


transaction_count = st.sidebar.number_input(
    "Number of Transactions",
    min_value=1,
    max_value=10000,
    value=4,
    step=1
)


run_button = st.sidebar.button(
    "▶ Run Fraud Detection",
    use_container_width=True
)


# =========================================================
# Run Pipeline
# =========================================================

if run_button:

    results_container = {}
    error_container = {}


    # -----------------------------------------------------
    # Consumer Worker
    # -----------------------------------------------------

    def consumer_worker():

        try:

            results_container["results"] = (
                consume_transactions(
                    int(transaction_count)
                )
            )

        except Exception as e:

            error_container["error"] = str(e)


    # -----------------------------------------------------
    # Run Consumer + Producer
    # -----------------------------------------------------

    with st.spinner(
        "Running real-time fraud detection..."
    ):

        # Start consumer first
        consumer_thread = threading.Thread(
            target=consumer_worker
        )

        consumer_thread.start()


        # Give consumer time to start listening
        time.sleep(3)


        # Send transactions
        sent_ids = send_transactions(
            int(transaction_count)
        )


        # Show result in sidebar
        st.sidebar.success(
            f"Sent {len(sent_ids)} transactions"
        )


        # Wait for consumer
        consumer_thread.join()


    # =====================================================
    # Error Handling
    # =====================================================

    if "error" in error_container:

        st.sidebar.error(
            error_container["error"]
        )

        st.stop()


    # =====================================================
    # Store Results
    # =====================================================

    if "results" in results_container:

        st.session_state.results = (
            results_container["results"]
        )


        st.sidebar.success(
            "✅ Fraud detection completed"
        )


# =========================================================
# Dashboard
# =========================================================

if "results" in st.session_state:

    df = st.session_state.results.copy()


    # =====================================================
    # Check Results
    # =====================================================

    if df.empty:

        st.warning(
            "No results found."
        )

        st.stop()


    # =====================================================
    # Remove Actual Label from Dashboard
    # =====================================================

    if "actual_label" in df.columns:

        df = df.drop(
            columns=["actual_label"]
        )


    # =====================================================
    # Summary Metrics
    # =====================================================

    total_transactions = len(df)


    fraud_count = (
        df["status"]
        .value_counts()
        .get(
            "Fraud",
            0
        )
    )


    normal_count = (
        df["status"]
        .value_counts()
        .get(
            "Normal",
            0
        )
    )


    avg_time = (
        df["processing_time_ms"]
        .mean()
    )


    # =====================================================
    # Transaction Summary
    # =====================================================

    st.subheader(
        "📌 Transaction Summary"
    )


    col1, col2, col3, col4 = st.columns(4)


    col1.metric(
        "Total Transactions",
        total_transactions
    )


    col2.metric(
        "🚨 Fraud Detected",
        fraud_count
    )


    col3.metric(
        "Normal Transactions",
        normal_count
    )


    col4.metric(
        "Average Latency",
        f"{avg_time:.2f} ms"
    )


    st.divider()


    # =====================================================
    # Suspicious Transactions + Fraud Probability Chart
    # =====================================================

    left_col, right_col = st.columns(
        [1, 1],
        gap="medium"
    )


    # =====================================================
    # Suspicious Transactions
    # =====================================================

    with left_col:

        st.subheader(
            "🚨 Suspicious Transactions"
        )


        fraud_df = df[
            df["status"] == "Fraud"
        ]


        if len(fraud_df) > 0:

            suspicious_display = fraud_df[
                [
                    "transaction_id",
                    "fraud_probability",
                    "processing_time_ms"
                ]
            ].copy()


            # Convert probability to percentage
            suspicious_display[
                "fraud_probability"
            ] = (
                suspicious_display[
                    "fraud_probability"
                ] * 100
            ).round(2)


            # Rename columns
            suspicious_display = (
                suspicious_display.rename(
                    columns={
                        "transaction_id":
                            "Transaction ID",

                        "fraud_probability":
                            "Fraud Probability (%)",

                        "processing_time_ms":
                            "Latency (ms)"
                    }
                )
            )


            st.dataframe(
                suspicious_display,
                use_container_width=True,
                height=240,
                hide_index=True
            )


        else:

            st.success(
                "No suspicious transactions detected."
            )


    # =====================================================
    # Fraud Probability Chart
    # =====================================================

    with right_col:

        st.subheader(
            "📈 Fraud Probability by Transaction"
        )


        chart_df = df[
            [
                "transaction_id",
                "fraud_probability"
            ]
        ].copy()


        # Convert probability to percentage
        chart_df[
            "fraud_probability"
        ] = (
            chart_df[
                "fraud_probability"
            ] * 100
        )


        # Presentation-friendly transaction numbers
        chart_df[
            "transaction_number"
        ] = range(
            1,
            len(chart_df) + 1
        )


        # -------------------------------------------------
        # Create Chart
        # -------------------------------------------------

        fig, ax = plt.subplots(
            figsize=(6, 2.6)
        )


        ax.plot(
            chart_df["transaction_number"],
            chart_df["fraud_probability"],
            marker="o",
            linewidth=2,
            markersize=6
        )


        # X-axis
        ax.set_xlabel(
            "Transaction",
            fontsize=9,
            fontweight="bold"
        )


        # Y-axis
        ax.set_ylabel(
            "Fraud Probability (%)",
            fontsize=9,
            fontweight="bold"
        )


        # Probability range
        ax.set_ylim(
            0,
            100
        )


        # X-axis ticks
        ax.set_xticks(
            chart_df[
                "transaction_number"
            ]
        )


        # Tick size
        ax.tick_params(
            axis="both",
            labelsize=8
        )


        # Grid
        ax.grid(
            True,
            alpha=0.3
        )


        fig.tight_layout()


        st.pyplot(
            fig,
            use_container_width=True
        )


        plt.close(fig)


    st.divider()


    # =====================================================
    # Transaction Monitoring + Download Report
    # =====================================================

    title_col, download_col = st.columns(
        [5, 1],
        gap="small"
    )


    with title_col:

        st.subheader(
            "🧾 Transaction Monitoring"
        )


    with download_col:

        csv = df.to_csv(
            index=False
        )


        st.download_button(
            label="⬇️ Download Report",
            data=csv,
            file_name="fraud_predictions.csv",
            mime="text/csv",
            use_container_width=True
        )


    # =====================================================
    # Transaction Monitoring Table
    # =====================================================

    monitoring_display = df.copy()


    # Convert fraud probability to percentage
    if "fraud_probability" in monitoring_display.columns:

        monitoring_display[
            "fraud_probability"
        ] = (
            monitoring_display[
                "fraud_probability"
            ] * 100
        ).round(2)


    # Rename columns
    monitoring_display = (
        monitoring_display.rename(
            columns={
                "transaction_id":
                    "Transaction ID",

                "fraud_probability":
                    "Fraud Probability (%)",

                "status":
                    "Status",

                "processing_time_ms":
                    "Processing Time (ms)"
            }
        )
    )


    # Show table
    st.dataframe(
        monitoring_display,
        use_container_width=True,
        height=320,
        hide_index=True
    )


# =========================================================
# Initial Message
# =========================================================

else:

    st.info(
        "Enter the number of transactions and click "
        "**Run Fraud Detection** to start real-time detection."
    )
