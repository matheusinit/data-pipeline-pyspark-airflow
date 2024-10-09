from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def start_task():
    print("Starting the data pipeline...")


def process_task():
    print("Processing the data...")


def end_task():
    print("Data pipeline completed.")


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 9),
    'retries': 1,
}
# Initialize the DAG
with DAG(
    'sample_data_pipeline',
    default_args=default_args,
    description='A simple data pipeline DAG',
    schedule_interval='@daily',  # You can change this to your desired schedule
) as dag:
    # Define the tasks
    start = PythonOperator(
        task_id='start',
        python_callable=start_task,
        dag=dag,
    )

    process = PythonOperator(
        task_id='process',
        python_callable=process_task,
        dag=dag,
    )

    end = PythonOperator(
        task_id='end',
        python_callable=end_task,
        dag=dag,
    )

    # Set the task dependencies
    start >> process >> end
