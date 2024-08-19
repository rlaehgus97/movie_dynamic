from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
    BranchPythonOperator
)
import os

with DAG(
    'parsing',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie_extract',
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2016, 1, 1),
    catchup=True,
    tags=['movie', 'extract'],
) as dag:


    def tmp():
        pass

    get_data = PythonVirtualenvOperator(
        task_id="get.data",
        python_callable=tmp,
        requirements=["git+https://github.com/rlaehgus97/movie_dynamic.git@d0.2.0/movie_flow"],
        system_site_packages=False,
    )

    parsing_parquet = PythonVirtualenvOperator(
        task_id="parsing.parquet",
        python_callable=tmp,
        requirements=["git+https://github.com/rlaehgus97/movie_dynamic.git@d0.2.0/movie_flow"],
        system_site_packages=False,
    )

    select_parquet = PythonVirtualenvOperator(
        task_id="select.parquet",
        python_callable=tmp,
        requirements=["git+https://github.com/rlaehgus97/movie_dynamic.git@d0.2.0/movie_flow"],
        system_site_packages=False,
    )

    start = EmptyOperator(
        task_id="start",
                )

    end = EmptyOperator(
        task_id="end",
                )
    

    start >> get_data >> parsing_parquet >> select_parquet >> end 
