from airflow.models import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.python_operator import PythonOperator
import random

args = {
    "owner": "ian",
    "start_date": days_ago(1)
}
dag = DAG(dag_id="SimpleDAG", default_args=args, schedule_interval=None)


def run_this_func(**context):
    rec_val = context['ti'].xcom_pull(key="val")
    print(f"rec_val: {rec_val}")


def push_to_xcom(**context):
    val = random.random()
    context['ti'].xcom_push(key="val", value=val)
    print('We gucci')


with dag:
    run_this_task = PythonOperator(
        task_id='run_this',
        python_callable=push_to_xcom,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=1)
    )

    run_this_task2 = PythonOperator(
        task_id='run_this2',
        python_callable=run_this_func,
        # Need to set this to true in order to push values to other tasks via xcom
        provide_context=True
    )

    # To order them to run sequentially, do this
    run_this_task >> run_this_task2
