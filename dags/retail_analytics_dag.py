"""
dags/retail_analytics_dag.py
----------------------------
Optional Apache Airflow DAG orchestrating the end-to-end Retail Analytics Pipeline:
Task Flow:
1. wait_for_daily_files: FileSensor or BashOperator checking data/generated/sales
2. ingest_bronze_layer: PythonOperator executing src.bronze.bronze_pipeline
3. transform_silver_layer: PythonOperator executing src.silver.silver_pipeline
4. build_gold_star_schema: PythonOperator executing src.gold.gold_pipeline
5. upload_to_s3: PythonOperator executing src.redshift.s3_uploader
6. load_redshift_dw: PythonOperator executing src.redshift.redshift_loader
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retail_sales_analytics_pipeline",
    default_args=default_args,
    description="End-to-End Retail ETL: CSV/Oracle/MySQL -> Bronze -> Silver -> Gold -> Redshift",
    schedule_interval="@daily",
    catchup=False,
    tags=["retail", "pyspark", "redshift", "medallion"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    def run_bronze():
        from src.bronze.bronze_pipeline import run_bronze_pipeline
        run_bronze_pipeline()

    def run_silver():
        from src.silver.silver_pipeline import run_silver_pipeline
        run_silver_pipeline()

    def run_gold():
        from src.gold.gold_pipeline import run_gold_pipeline
        run_gold_pipeline()

    def run_s3_upload():
        from src.redshift.s3_uploader import upload_gold_to_s3
        upload_gold_to_s3()

    def run_redshift_load():
        from src.redshift.redshift_loader import run_redshift_pipeline
        run_redshift_pipeline()

    task_bronze = PythonOperator(
        task_id="ingest_bronze_layer",
        python_callable=run_bronze,
    )

    task_silver = PythonOperator(
        task_id="transform_silver_layer",
        python_callable=run_silver,
    )

    task_gold = PythonOperator(
        task_id="build_gold_star_schema",
        python_callable=run_gold,
    )

    task_s3_upload = PythonOperator(
        task_id="upload_gold_to_s3",
        python_callable=run_s3_upload,
    )

    task_redshift_load = PythonOperator(
        task_id="load_redshift_dw",
        python_callable=run_redshift_load,
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Pipeline task dependency DAG definition
    start_pipeline >> task_bronze >> task_silver >> task_gold >> task_s3_upload >> task_redshift_load >> end_pipeline
