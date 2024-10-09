from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

spark: SparkSession = SparkSession.builder \
    .appName("SimplePipeline") \
    .master('spark://spark:8080') \
    .getOrCreate()


def run_spark_job():
    df = spark.read.csv("./dataset.csv", header=True, inferSchema=True)

    df.show(5)

    spark.stop()


def end_task():
    print("Data pipeline completed.")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 9),
    'retries': 1,
}

with DAG(
    default_args=default_args,
    dag_id="spark-runner",
    schedule_interval="@daily"
) as dag:
    run_spark_job_task = PythonOperator(
        task_id="run_spark_job",
        python_callable=run_spark_job,
        dag=dag
    )

    end = PythonOperator(
        task_id='run_end_task',
        python_callable=end_task,
        dag=dag
    )

    run_spark_job_task >> end_task
