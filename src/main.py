from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.python import PythonOperator

spark: SparkSession = SparkSession.builder \
    .appName("SimplePipeline") \
    .master('spark://localhost:8081') \
    .config("spark.network.maxFrameSize", "200m") \
    .getOrCreate()


def run_spark_job():
    df = spark.read.csv("./dataset.csv", header=True, inferSchema=True)

    df.show(5)

    spark.stop()


with DAG(
    dag_id="spark-runner",
    schedule="@daily"
) as dag:
    run_spark_job_task = PythonOperator(
        task_id="run_spark_job",
        python_callable=run_spark_job
    )

    run_spark_job_task
