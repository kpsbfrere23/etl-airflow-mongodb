from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from pymongo import MongoClient

MONGO_URI = "mongodb://mongo:27017"
DB_NAME = "user_data"
COLLECTION_NAME = "users"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def fetch_and_store_users():
    # Get users from API
    res = requests.get("https://randomuser.me/api/?results=10")
    res.raise_for_status()
    users = res.json()['results']

    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    new_users = []
    for user in users:
        uuid = user['login']['uuid']
        # Check if user already exists
        if not collection.find_one({'login.uuid': uuid}):
            new_users.append(user)

    if new_users:
        collection.insert_many(new_users)
        print(f"Inserted {len(new_users)} new users.")
    else:
        print("No new users to insert.")

with DAG(
    dag_id='incremental_user_ingestion',
    default_args=default_args,
    description='Ingests new random users on a schedule with deduplication',
    schedule_interval='@hourly',  # You can change this to '@daily', '*/30 * * * *', etc.
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'mongo', 'users'],
) as dag:

    ingest_task = PythonOperator(
        task_id='fetch_and_insert_users',
        python_callable=fetch_and_store_users
    )

    ingest_task

