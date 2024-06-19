from datetime import datetime, timedelta
import pytz
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
import logging

# AWS credentials
AWS_ACCESS_KEY_ID = 'your-access-key-id'
AWS_SECRET_ACCESS_KEY = 'your-secret-access-key'
AWS_REGION = 'eu-central-1'

# Function to upload current weather CSV to S3
def upload_to_s3(file_path, bucket_name, s3_key):
    try:
        # Initialize a session using Amazon S3 with credentials
        s3 = boto3.client(
        's3',
        aws_access_key_id='your-access-key-id', #hardcode the values
        aws_secret_access_key='your-secret-access-key', #hardcode the values
        region_name='eu-central-1' #hardcode the values
        )

        bucket_name = 'myfirstbucketsoumya'
        file_key = 'hourly_berlin_weather.txt'
    
        # Upload CSV file to S3
        s3.upload_file(file_path, bucket_name, s3_key)
        logging.info(f"Uploaded weather data to S3: s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logging.error(f"Failed to upload to S3: {e}")

# Function to update weather data and save to CSV
def update_weather(**kwargs):
    url = 'https://weather.com/weather/today/l/52.52,13.40'
    page = requests.get(url)
    if page.status_code != 200:
        raise Exception(f"Failed to fetch page: {page.status_code}")

    soup = BeautifulSoup(page.text, 'html.parser')

    # Find the title for the feels-like temperature
    title_element = soup.find(class_='TodayDetailsCard--feelsLikeTempLabel--1UNV1')
    if title_element:
        title = title_element.text.strip()
    else:
        title = "Feels Like Temperature"

    # File path to save the DataFrame
    file_path = '/opt/airflow/dags/weather_checkin.csv'  

    # Check if the file exists
    if os.path.exists(file_path):
        logging.info(f"File {file_path} exists. Loading existing DataFrame.")
        # Load the existing DataFrame
        current_weather_berlin_df = pd.read_csv(file_path)
    else:
        logging.info(f"File {file_path} does not exist. Creating new DataFrame.")
        # Initialize a new DataFrame with the column title and date_time
        current_weather_berlin_df = pd.DataFrame(columns=[title, 'date_time'])

    # Find the feels-like temperature value
    value_element = soup.find('span', class_='TodayDetailsCard--feelsLikeTempValue--2icPt')
    if value_element:
        feels_like_temp = value_element.text.strip()
    else:
        feels_like_temp = None

    # Get the current date and time in Berlin timezone
    berlin_tz = pytz.timezone('Europe/Berlin')
    current_datetime = datetime.now(berlin_tz).strftime("%Y-%m-%d %H:%M:%S")

    # Append the data to the DataFrame
    if feels_like_temp:
        logging.info(f"Appending new data: {feels_like_temp}, {current_datetime}")
        new_data = {title: [feels_like_temp], 'date_time': [current_datetime]}
        current_weather_berlin_df = current_weather_berlin_df._append(pd.DataFrame(new_data), ignore_index=True)
    else:
        logging.error("Failed to find the feels-like temperature value")

    # Save the DataFrame to the CSV file
    logging.info(f"Saving DataFrame to {file_path}")
    current_weather_berlin_df.to_csv(file_path, index=False)
    logging.info(current_weather_berlin_df)

    # Upload CSV to S3
    bucket_name = 'myfirstbucketsoumya'  # Replace with your S3 bucket name
    s3_key = 'current_weather_berlin.csv'  # Replace with desired S3 key
    upload_to_s3(file_path, bucket_name, s3_key)

# Define a DAG
DAG_NAME = 'berlin-weather'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id=DAG_NAME,
    description='Berlin weather every hour',
    schedule_interval='@hourly',
    default_args=default_args,
    catchup=False,
)

# Define tasks
update_weather_task = PythonOperator(
    task_id='update_weather',
    python_callable=update_weather,
    dag=dag,
)

# Task dependencies
update_weather_task

