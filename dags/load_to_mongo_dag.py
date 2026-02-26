from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import pandas as pd

PROCESSED_FILE_PATH = "/opt/airflow/data/processed/processed.csv"

processed_dataset = Dataset("file:///opt/airflow/data/processed/processed.csv")


def load_to_mongo():
    df = pd.read_csv(PROCESSED_FILE_PATH)

    mongo = MongoHook(conn_id="mongo_default")

    client = mongo.get_conn()
    db = client["reviews_db"]
    collection = db["reviews"]

    collection.delete_many({})  # optional: clear old data
    collection.insert_many(df.to_dict("records"))


with DAG(
    dag_id="load_to_mongo_dag",
    start_date=datetime(2026, 2, 26),
    schedule=[processed_dataset],  # 🔥 dataset-triggered
    catchup=False,
    tags=["reviews", "mongo"],
) as dag:

    load_task = PythonOperator(
        task_id="load_processed_data_to_mongo",
        python_callable=load_to_mongo,
    )

    load_task