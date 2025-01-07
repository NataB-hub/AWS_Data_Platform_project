import os
import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.msqlserver_pipeline import sql_server_pipeline
from pipelines.aws_s3_pipeline import upload_s3_pipeline

default_args = {
    'owner': 'Natalia Batsei',
    'start_date': datetime(2025, 1, 7)
}

file_postfix = datetime.now().strftime("%Y%m%d")
dag = DAG(
    dag_id='etl_sql_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['sql', 'etl', 'pipeline']
)

extract = PythonOperator(
    task_id='sql_extraction',
    python_callable=sql_server_pipeline,
    op_kwargs={
        'file_postfix': f'{file_postfix}',
        'schema_tables': [{'schema': 'Sales',
                           'tables': ['SalesOrderHeader']},
                           {'schema': 'HumanResources',
                            'tables': ['Employee', 'Department', 'EmployeePayHistory']}]
    },
    dag=dag
)

upload_s3 = PythonOperator(
    task_id = 's3_upload',
    python_callable = upload_s3_pipeline,
    dag = dag
)

extract >> upload_s3

