import os
import datetime
import pandas as pd
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.decorators import task

with DAG(
        dag_id="lab1_dag1",
        schedule_interval=None,
        dagrun_timeout=datetime.timedelta(minutes=40),
        start_date=datetime.datetime(2024, 1, 1),
        catchup=False,
) as dag:

    @task(task_id='read')
    def read():
        input_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'input')
        df = pd.concat([pd.read_csv(os.path.join(input_dir, f'chunk{i}.csv')) for i in range(26)], axis=0)
        df.to_csv('tmp.csv', index=False)


    @task(task_id='drop')
    def drop():
        df = pd.read_csv('tmp.csv')
        df.dropna(subset=['designation', 'region_1'], inplace=True, axis=0)
        df.to_csv('tmp.csv', index=False)


    @task(task_id='fill')
    def fill():
        df = pd.read_csv('tmp.csv')
        df.fillna({'price': 0.0})
        df.to_csv('tmp.csv', index=False)


    @task(task_id='save_csv')
    def save_csv():
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'output')
        pd.read_csv('tmp.csv').to_csv(os.path.join(output_dir, 'output.csv'), index=False)


    @task(task_id='save_els')
    def save_els():
        df = pd.read_csv('tmp.csv')
        es = Elasticsearch("http://elasticsearch-kibana:9200")
        for _, row in df.iterrows():
            es.index(index='lab1_dag1_v2', body=row.to_json())


    task1, task2, task3, task4_1, task4_2 = read(), drop(), fill(), save_csv(), save_els()

    task1 >> task2 >> task3 >> [task4_1, task4_2]