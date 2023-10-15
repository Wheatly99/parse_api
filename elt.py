from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

import requests
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1
}

dag = DAG(
    'ELT',
    default_args=default_args,
    schedule='1 */12 * * *',
    catchup=False
)

def get_and_save_data_from_first_json(api, db):
  response = requests.get(api)
  df = pd.DataFrame.from_dict(response.json(), orient='columns')
  engine = create_engine(db)
  df.to_sql('from_api_1', engine, if_exists='append')

def get_and_save_data_from_second_json(api, db):
  response = requests.get(api)

  df1 = pd.json_normalize(response.json(), record_path=['stats'])
  df2 = pd.json_normalize(response.json(), record_path=['stats', 'splits'], meta=['copyright'])
  df = df1.merge(df2, how='inner', left_index=True, right_index=True).drop(columns={'splits'})

  engine = create_engine(db)
  df.to_sql('from_api_2', engine, if_exists='append')

start = DummyOperator(task_id='start_task', dag=dag)

ELT1 = PythonOperator(
    task_id='extract_load_1',
    python_callable=get_and_save_data_from_first_json,
    op_args=['https://random-data-api.com/api/cannabis/random_cannabis?size=10', 'postgresql://Wheatly99:hAQ4a1orCJNm@ep-little-recipe-11390401.us-east-2.aws.neon.tech/data'],
    dag=dag
)

ELT2 = PythonOperator(
    task_id='extract_load_2',
    python_callable=get_and_save_data_from_second_json,
    op_args=['https://statsapi.web.nhl.com/api/v1/teams/21/stats', 'postgresql://Wheatly99:hAQ4a1orCJNm@ep-little-recipe-11390401.us-east-2.aws.neon.tech/data'],
    dag=dag
)

end = DummyOperator(task_id='end_task', dag=dag)

start >> [ELT1, ELT2] >> end
