import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="my_dag_name3",  
    start_date=datetime.datetime(2021, 1, 1),  
    schedule="@daily",
):

    tarea = EmptyOperator(task_id="task")

