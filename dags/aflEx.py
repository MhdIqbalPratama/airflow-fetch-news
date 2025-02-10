import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas  as pd
from datetime import timedelta, datetime
import sqlite3
import xml.etree.ElementTree as ET

default_args = {
    'owner': 'admin11',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

load_dotenv()
url = "https://www.cnnindonesia.com/rss"
db_path = os.getenv('db_path')

# Define functions
def fetch_data_task(**kwargs):
    try:
        res = requests.get(url)
        res.raise_for_status()  # Raise an exception for bad status codes
        root = ET.fromstring(res.content)
        rows = []
        for item in root.findall('.//item'):
            title = item.find('title').text
            link = item.find('link').text
            description = item.find('description').text
            pub_date = item.find('pubDate').text
            rows.append({'title': title, 'link': link, 'description': description, 'pub_date': pub_date})
        # Pass data to next task using XCom
        kwargs['ti'].xcom_push(key='news_data', value=rows)
        print("Data fetched successfully")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        raise
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        raise
    except AttributeError as e:
        print(f"Error extracting data: {e}")
        raise

def transform_to_dataframe_task(**kwargs):
    # Get data from XCom
    data = kwargs['ti'].xcom_pull(key='news_data', task_ids='fetch_data')
    if not data:
        print("No data fetched, skipping transformation")
        return
    try:
        df = pd.DataFrame(data)
        df['pub_date'] = pd.to_datetime(df['pub_date'])
        # Connect to SQLite database and store data
        conn = sqlite3.connect(db_path)
        df.to_sql("cnn_news", conn, if_exists="append", index=False)
        conn.commit()
        print("Data loaded to database successfully")
        conn.close()
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        raise
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        raise

with DAG(
    'cnn_news_pipeline',
    default_args=default_args,
    description='A simple pipeline to fetch and store CNN news RSS feed',
    schedule_interval=timedelta(days=1),
    start_date=datetime.now(),
    catchup=False,
    tags=['cnn', 'rss', 'pipeline'],
) as dag:
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_task,
        provide_context=True
    )
    transform_to_dataframe = PythonOperator(
        task_id='transform_to_dataframe',
        python_callable=transform_to_dataframe_task,
        provide_context=True
    )

    # Define task dependencies
    fetch_data >> transform_to_dataframe