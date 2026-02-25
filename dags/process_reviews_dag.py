from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from datetime import datetime
import pandas as pd
import os
import re

RAW_FILE_PATH = "/opt/airflow/data/raw/tiktok_google_play_reviews.csv"
PROCESSED_FILE_PATH = "/opt/airflow/data/processed/processed.csv"

# Dataset definition (used to trigger second DAG)
processed_dataset = Dataset("file:///opt/airflow/data/processed/processed.csv")


# ---------- BRANCH FUNCTION ----------
def check_if_file_empty():
    if not os.path.exists(RAW_FILE_PATH):
        return "file_empty"

    df = pd.read_csv(RAW_FILE_PATH)

    if df.empty:
        return "file_empty"
    return "data_processing.replace_nulls"


# ---------- TASK FUNCTIONS ----------
def replace_nulls():
    df = pd.read_csv(RAW_FILE_PATH)
    df.fillna("-", inplace=True)
    df.to_csv("/opt/airflow/data/processed/step1.csv", index=False)


def sort_by_date():
    df = pd.read_csv("/opt/airflow/data/processed/step1.csv")
    df["at"] = pd.to_datetime(df["at"], errors="coerce")
    df.sort_values(by="at", inplace=True)
    df.to_csv("/opt/airflow/data/processed/step2.csv", index=False)


def clean_content():
    df = pd.read_csv("/opt/airflow/data/processed/step2.csv")

    df["content"] = df["content"].apply(
        lambda x: re.sub(r"[^a-zA-Z0-9\s\.,!?]", "", str(x))
    )

    df.to_csv(PROCESSED_FILE_PATH, index=False)


# ---------- DAG DEFINITION ----------
with DAG(
    dag_id="process_reviews_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["reviews", "processing"],
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=RAW_FILE_PATH,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=600,
        mode="poke",
    )

    branch_task = BranchPythonOperator(
        task_id="check_if_empty",
        python_callable=check_if_file_empty,
    )

    file_empty = BashOperator(
        task_id="file_empty",
        bash_command='echo "The file is empty!"',
    )

    with TaskGroup("data_processing") as data_processing:

        t1 = PythonOperator(
            task_id="replace_nulls",
            python_callable=replace_nulls,
        )

        t2 = PythonOperator(
            task_id="sort_by_date",
            python_callable=sort_by_date,
        )

        t3 = PythonOperator(
            task_id="clean_content",
            python_callable=clean_content,
            outlets=[processed_dataset],  # 🔥 triggers second DAG
        )

        t1 >> t2 >> t3

    wait_for_file >> branch_task
    branch_task >> file_empty
    branch_task >> data_processing