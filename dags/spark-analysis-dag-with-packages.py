from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator # type: ignore
from airflow.utils.dates import days_ago
import os

# Access environment variables
POSTGRES_HOST = os.getenv("POSTGRES_CONTAINER_NAME")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DATABASE = os.getenv("POSTGRES_DW_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD") 

default_args = {
    "owner": "hafidz",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="pyspark_retail_analysis_with_package",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Assignment retail analysis using packages as postgres lib",
    start_date=days_ago(1),
)

Extract = SparkSubmitOperator(
    application="/spark-scripts/spark-analysis-with-package-script.py",
    conn_id="spark_main",
    task_id="run_pyspark_analysis_with_package",
    dag=spark_dag, 
    packages="org.postgresql:postgresql:42.2.18",
    env_vars={
        "POSTGRES_USER": POSTGRES_USER, 
        "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
        "POSTGRES_HOST": POSTGRES_HOST,
        "POSTGRES_PORT": POSTGRES_PORT,
        "POSTGRES_DATABASE": POSTGRES_DATABASE,
        },
)

Extract
