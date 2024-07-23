import pandas as pd
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from airflow.utils.task_group import TaskGroup
import os
import zipfile
import logging

# Define the new data path within your home directory
home_dir = os.path.expanduser('~')
data_path = os.path.join(home_dir, 'data')

# Ensure the directory exists
if not os.path.exists(data_path):
    os.makedirs(data_path)



default_args = {
    'owner': 'pduy',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

table_list = ['sellers', 'product_category_name_translation', 'orders', 'order_items',
              'customers', 'geolocation', 'order_payments', 'order_reviews', 'products']


def csvToPostgres(table_name):
    try:
        # Open Postgres Connection
        connection = PostgresHook.get_connection("pg_local")
        # Get PostgreSQL URI
        URI = connection.get_uri().replace('postgres://', 'postgresql://')
        logging.info(f'PostgreSQL URI: {URI}')
        
        # Create SQLAlchemy engine
        conn = create_engine(URI)
        
        # Read CSV file
        if table_name == 'product_category_name_translation':
            data_file_path = f'{data_path}/{table_name}.csv'
        else:
            data_file_path = f'{data_path}/olist_{table_name}_dataset.csv'
        
        logging.info(f'Reading CSV file from: {data_file_path}')
        
        data = pd.read_csv(data_file_path)
        
        # Load CSV data into PostgreSQL table
        logging.info(f'Loading data into table: {table_name}')
        data.to_sql(table_name, conn, if_exists='replace', index=False)
        
        logging.info(f'Successfully loaded data into table: {table_name}')
    except Exception as e:
        logging.error(f'Error in loading data into PostgreSQL table {table_name}: {e}')
        raise

def unzip_file(src, dest):
    with zipfile.ZipFile(src, 'r') as zip_ref:
        zip_ref.extractall(dest)


with DAG(
        dag_id='olist_local_pg_etl',
        schedule_interval='@daily',
        start_date=datetime(year=2024, month=1, day=1),
        catchup=False
) as dag:
    task_get_data = BashOperator(task_id='get_data',
                                 bash_command=f'kaggle datasets download olistbr/brazilian-ecommerce -p {data_path}'
                                )


    task_check_file_exists = FileSensor(task_id='check_file_exists',
                                        filepath=f'{data_path}/brazilian-ecommerce.zip',
                                        fs_conn_id='my_file_path')

    task_extract_zip = PythonOperator(
        task_id='extract_zip',
        python_callable=unzip_file,
        op_kwargs={
            'src': f'{data_path}/brazilian-ecommerce.zip',
            'dest': data_path
        }
    )

    task_check_sub_file_exists = FileSensor(task_id='check_sub_file_exists',
                                            filepath=f'{data_path}/olist_customers_dataset.csv',
                                            fs_conn_id='my_file_path')

    with TaskGroup("load", dag=dag) as load:
        for table_name in table_list:
            table_name = PythonOperator(
                default_args=default_args,
                task_id=table_name,
                python_callable=csvToPostgres,
                op_kwargs={'table_name': table_name},
                dag=dag)

task_get_data >> task_check_file_exists >> task_extract_zip >> task_check_sub_file_exists >> load