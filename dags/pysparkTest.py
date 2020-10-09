import pyspark
from airflow.models import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.python_operator import PythonOperator
import random

args = {
    "owner": "ian",
    "start_date": days_ago(1)
}
dag = DAG(dag_id="pysparkTest", default_args=args, schedule_interval=None)


def run_this_func(**context):
    sc = pyspark.SparkContext('local[*]')
    print(sc)

with dag:
    run_this_task = PythonOperator(
        task_id='run_this',
        python_callable=push_to_xcom,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=1)
    )
