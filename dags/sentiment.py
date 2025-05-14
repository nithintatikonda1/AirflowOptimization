import os
import logging
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import boto3
import json
from airflowfusion.backend_registry import read, write
from airflowfusion.fuse import create_optimized_dag

# Setup for logging
task_logger = logging.getLogger("airflow.task")

# Constants
S3_BUCKET_NAME = os.getenv("S3_BUCKET")  # S3 bucket name
HUGGINGFACE_API_TOKEN = os.getenv("HUGGINGFACE_API_TOKEN")
SENTIMENT_ANALYSIS_MODEL = "cardiffnlp/twitter-roberta-base-sentiment"
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID') 
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY') 


def get_sentences_from_api(**kwargs):
    """
    Get a random joke from the Manatee Joke API and store it to an S3 bucket.
    """
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)

    r = requests.get("https://manateejokesapi.herokuapp.com/manatees/random")
    df = pd.json_normalize(r.json())
    print("DF SIZE: ", len(df))
    df.columns = [col_name.upper() for col_name in df.columns]
    df = df.rename(columns={"ID": "JOKE_ID"})

    # Save the data to S3 as CSV (or JSON, or any other format)
    file_path = '/tmp/jokes_data.csv'  # Temporary file path to store CSV data
    df.to_csv(file_path, index=False)

    # Upload to S3
    s3_client.upload_file(file_path, S3_BUCKET_NAME, 'jokes/jokes_data.csv')

    task_logger.info(f"Data uploaded to S3: s3://{S3_BUCKET_NAME}/jokes/jokes_data.csv")
    write('xcom', 'file_path', file_path)

def transform(**kwargs):
    """
    Transform the incoming file to select 'setup' and 'punchline' columns.
    """
    input_file_path = read('xcom', 'file_path')
    df = pd.read_csv(input_file_path)
    write('xcom', 'df', df[["SETUP", "PUNCHLINE"]])

def sentiment_analysis(huggingface_api_token: str, model_name: str, **kwargs):
    """
    Run a sentiment analysis on the setup and punchline of the joke using HuggingFace API.
    """
    df = read('xcom', 'df')
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)
    headers = {"Authorization": f"Bearer {huggingface_api_token}"}

    query = list(df["SETUP"].values) + list(df["PUNCHLINE"].values)

    api_url = f"https://api-inference.huggingface.co/models/{model_name}"

    response = requests.post(api_url, headers=headers, json={"inputs": query})

    if response.status_code == 200:
        response_text = response.json()
        task_logger.info(response_text)
    else:
        task_logger.error(
            f"Request failed with status code {response.status_code}: {response.text}"
        )
    
    # Store sentiment analysis results to S3 as JSON
    sentiment_data = response.json()  # Assuming response is in JSON format
    sentiment_file_path = '/tmp/sentiment_analysis.json'  # Temporary file path
    with open(sentiment_file_path, 'w') as f:
        json.dump(sentiment_data, f)

    # Upload to S3
    s3_client.upload_file(sentiment_file_path, S3_BUCKET_NAME, 'sentiment/sentiment_analysis.json')
    task_logger.info(f"Sentiment analysis result uploaded to S3: s3://{S3_BUCKET_NAME}/sentiment/sentiment_analysis.json")


# Define the DAG
with DAG(
    dag_id='manatee_sentiment',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sentiment", "manatee", "huggingface"],
) as dag:

    # Define the start operator
    start = EmptyOperator(task_id="start")

    # ------------------------- #
    # Ingest and transform data #
    # ------------------------- #

    # Get random joke from API and save to S3
    in_data = PythonOperator(
        task_id="get_sentences_from_api",
        python_callable=get_sentences_from_api,
    )

    # Transform the data (in this case, selecting setup and punchline columns)
    transformed_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    # ---------------------- #
    # Run sentiment analysis #
    # ---------------------- #

    run_model = PythonOperator(
        task_id="run_sentiment_analysis",
        python_callable=sentiment_analysis,
        op_kwargs={
            "huggingface_api_token": HUGGINGFACE_API_TOKEN,
            "model_name": SENTIMENT_ANALYSIS_MODEL,
        },
    )

    # Task dependencies
    start >> in_data
    in_data >> transformed_data
    transformed_data >> run_model

    fused_dag = create_optimized_dag(dag, parallelize=False)
    optimized_dag = create_optimized_dag(dag)