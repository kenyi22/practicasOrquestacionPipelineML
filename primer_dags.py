from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def hello_world():
    print("Hola, mundo!")

dag = DAG(
    dag_id='ejemplo_dag',
    description='Un ejemplo de DAG',
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
)

start = DummyOperator(task_id='start', dag=dag)
hello_task = PythonOperator(task_id='hello_task', python_callable=hello_world, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> hello_task >> end