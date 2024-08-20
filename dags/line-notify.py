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
    BranchPythonOperator,
)

with DAG(
        'line_notify',
    default_args={
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(seconds=3)
    },
    description='line message notification depended on job',
    schedule="@once",
    start_date=datetime(2024, 8, 1),
    end_date=datetime(2024, 8, 2),
    catchup=True,
    tags=['pyspark', 'line', 'data'],
) as dag:
    
    def gen_emp(id,rule="all_success"):
        op = EmptyOperator(task_id = id, trigger_rule=rule)
        return op
    
    def random():
        import random as r
        p = r.randint(0,1)
        
        if p == 1: #True
            print("task success")
            return notify.success
        else:
            print("task failed")
            return notify.fail

    LINE_TOKEN = u4zXpaSilNgvS9wIYS2ICzS7ydgQbiMJMRav5ho2rY6

    start = gen_emp(id="start")
    end = gen_emp(id="end")
    
    task_branch = BranchPythonOperator(
        task_id = "bash.job",
        python_callable = random,
        )
    
    task_ifsuccess = BashOperator(
        task_id="notify.success",
        bash_command="""
            curl -X POST -H 'Authorization: Bearer $LINE_TOKEN' -F 'task successed' https://notify-api.line.me/api/notify
        """
        )

    task_iffail = BashOperator(
        task_id="notify.fail",
        bash_command="""
            curl -X POST -H 'Authorization: Bearer $LINE_TOKEN' -F 'task failed' https://notify-api.line.me/api/notify
        """
        )

    start >> task_branch 
    task_branch >> [task_ifsuccess, task_iffail] >> end
