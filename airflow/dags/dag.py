from airflow import DAG
from airflow.operators.python import PythonOperator 
from random import randint

print('dag imports are working')

def _training_model():
    return randint(1,10)

