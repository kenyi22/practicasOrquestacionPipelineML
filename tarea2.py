import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import numpy as np
import pandas as pd


def list_ord():
    list_desordenada = np.random.randint(1, 20, size=10).tolist()
    print(list_desordenada)
    lista_ordenada = np.sort(list_desordenada).tolist()
    print(lista_ordenada)

def df_expor_csv():
    dfSimple=pd.DataFrame({'A': [1,2,3,4], 'B': [5,6,7,8], 'C': [9,10,11,12]})
    dfSimple.to_csv('dfSimple.csv', index=False)
    print("archivo generado")

def df_mult_matriz():
    matrix1 = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    matrix2 = np.array([[9, 8, 7], [6, 5, 4], [3, 2, 1]])
    resultado = np.dot(matrix1, matrix2)
    print(resultado)

my_dag = DAG(
    dag_id="my_dag_tarea2",  
    start_date=datetime.datetime(2021, 1, 1),  
    schedule="@daily",
)

tarea1 = BashOperator(
            task_id="task1", 
            bash_command="mkdir carpetaNueva && echo \"Mi primer Dag\" > carpetaNueva/archivo.txt", 
            dag=my_dag)
tarea4 = PythonOperator(
            task_id="task4", 
            python_callable=list_ord, 
            dag=my_dag)
tarea3 = PythonOperator(task_id="task3", python_callable=df_expor_csv, dag=my_dag)
tarea2 = PythonOperator(task_id="task2", python_callable=df_mult_matriz, dag=my_dag)

tarea2 >> tarea4 >> tarea3 >> tarea1
