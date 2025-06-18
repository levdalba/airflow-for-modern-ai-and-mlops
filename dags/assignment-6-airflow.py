from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split


def load_data_task():
    """Load and process the Iris dataset."""
    iris_data = load_iris(as_frame=True)
    dataset = iris_data.frame
    features = dataset.copy()
    features.rename(
        columns=lambda s: s.replace("(cm)", "").strip().replace(" ", "_"), inplace=True
    )
    features.to_csv("/opt/airflow/data/features_iris.csv", index=False)


def split_data_task(test_size=0.2):
    """Split the dataset into train and test sets."""
    dataset = pd.read_csv("/opt/airflow/data/features_iris.csv")
    df_train, df_test = train_test_split(dataset, test_size=test_size, random_state=42)
    df_train.to_csv("/opt/airflow/data/train.csv", index=False)
    df_test.to_csv("/opt/airflow/data/test.csv", index=False)


with DAG(
    dag_id="assignment-6-airflow",
    start_date=datetime(2023, 1, 1),
    schedule="* * * * *",  # Changed from schedule_interval to schedule
    catchup=False,
) as dag:
    load_data = PythonOperator(task_id="load_data", python_callable=load_data_task)

    split_data = PythonOperator(
        task_id="split_data",
        python_callable=split_data_task,
        op_kwargs={"test_size": 0.2},
    )

    load_data >> split_data
