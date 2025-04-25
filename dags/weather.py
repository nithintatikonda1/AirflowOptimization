import json
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import boto3
import io 
import json
import pickle
import xgboost as xgb
from openmeteo_py import Hourly, Options, OWmanager
from datetime import datetime
from io import StringIO, BytesIO
from airflowfusion.backend_registry import read, write
import numpy as np
from airflowfusion.operator import ParallelFusedPythonOperator
from airflowfusion.fuse import create_optimized_dag


def get_weather_data(filename, bucket_name, latitude=14.5995, longitude=120.9842, past_days=20, timezone="Asia/Shanghai"):
    """
    Retrieves weather data from OpenWeather API and returns a pandas dataframe with relevant columns.

    Args:
    - latitude: float - The latitude of the location you want to retrieve data for
    - longitude: float - The longitude of the location you want to retrieve data for
    - past_days: int - The number of days of past data you want to retrieve
    - timezone: str - The timezone of the location you want to retrieve data for

    Returns:
    - pandas dataframe - A dataframe with the relevant weather data columns
    """
    # Set up API options and download data
    hourly = Hourly()
    options = Options(latitude, longitude, past_days=past_days, timezone=timezone)
    mgr = OWmanager(options, hourly.all())
    meteo = mgr.get_data()

    # Convert to pandas dataframe
    df = pd.DataFrame(meteo['hourly'])

    # Get only data equal to or less than current time
    current_time = datetime.now().strftime('%Y-%m-%dT%H:%M')
    df = df[df["time"] <= current_time]

    # Define columns to retain for the model to predict temperature
    columns_to_keep = [
        "time",
        "apparent_temperature",
        "relativehumidity_2m",
        "dewpoint_2m",
        "pressure_msl",
        "cloudcover",
        "windspeed_10m",
        "precipitation",
        "direct_radiation",
        "soil_temperature_0cm"
    ]
    df = df[columns_to_keep]

    # Create an S3FileSystem object with the IAM role specified
    s3 = boto3.client('s3')

    bucket_name = bucket_name
    key = f"{filename}.csv"

    # Write data to a file on S3
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    #s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=key)
    write("s3", key, csv_buffer.getvalue(), s3, bucket_name)

def remove_id_column(df):
    """
    Removes the ID column from a pandas dataframe.
    Takes into account different spellings of "ID" and "Unnamed: 0".
    Parameters:
        df (pandas.DataFrame): The input dataframe.
    Returns:
        pandas.DataFrame: The dataframe with the ID column removed.
    """
    id_columns = ['ID', 'Id', 'id', 'Unnamed: 0']
    for col in id_columns:
        if col in df.columns:
            return df.drop(columns=[col])
    return df

def create_lag_window(df, num_lags=3, delay=1):
    """
    Adds lag and rolling window columns to the input dataframe.
    
    Parameters:
        df (pandas.DataFrame): input dataframe
        num_lags (int): number of lagged columns to create (default: 3)
        delay (int): time delay for lagged columns (default: 1)
        
    Returns:
        pandas.DataFrame: dataframe with added lag and rolling window columns
    """
    # Set the time column as the index
    print(df.columns)
    df.set_index("time", inplace=True)
    df = remove_id_column(df)

    for column in df:
        for lag in range(1,num_lags+1):
            df[column + '_lag' + str(lag)] = df[column].shift(lag*-1-(delay-1))
            df[column + '_avg_window_length' + str(lag+1)] = df[column].shift(-1-(delay-1)).rolling(window=lag+1,center=False).mean().shift(1-(lag+1))

    df.dropna(inplace=True) 

    mask = (df.columns.str.contains('apparent_temperature') | df.columns.str.contains('lag') | df.columns.str.contains('window'))
    df_processed = df[df.columns[mask]]
    
    return df_processed

def preprocess_data(filename, bucket_name):
    # Connect to S3
    s3 = boto3.client('s3')

    filename = f"{filename}.csv"

    # Get the weather data file from S3
    #obj = s3.get_object(Bucket=bucket_name, Key=filename)
    obj = read("s3", filename, s3, bucket_name)

    # Load the weather data into a Pandas DataFrame
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df = remove_id_column(df)

    df_processed = create_lag_window(df)
    df_processed = df_processed.reset_index(drop=True)

    bucket_name = bucket_name
    key = f"processed_{filename}"

    # Write data to a file on S3
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    #s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=key)
    write("s3", key, csv_buffer.getvalue(), s3, bucket_name)

def train_model(filename, bucket_name, target_column):
    # Connect to S3
    s3 = boto3.client('s3')

    filename = f"processed_{filename}.csv"
    #obj = s3.get_object(Bucket=bucket_name, Key=filename)
    obj = read("s3", filename, s3, bucket_name)

    # Load the processed weather data into a Pandas DataFrame
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df = remove_id_column(df)

    X = df.drop(target_column, axis=1)
    y = df[target_column]

    # Train the XGBoost model based on updated data
    model = xgb.XGBRegressor(objective="reg:squarederror", random_state=42)
    model.fit(X, y)

    # Serialize the model to a byte stream
    model_bytes = pickle.dumps(model, protocol=2)
    key = "model.pkl"

    #s3.put_object(Body=model_bytes, Bucket=bucket_name, Key=key)
    write("s3", key, model_bytes, s3, bucket_name)

def make_predictions(model_path, filename, bucket_name, target_column):
    # Connect to S3
    s3 = boto3.client('s3')

    # Get the model and processed weather data file from S3
    model_obj = s3.get_object(Bucket=bucket_name, Key=model_path)
    filename = f"processed_{filename}.csv"
    #obj = s3.get_object(Bucket=bucket_name, Key=filename)
    obj = read("s3", filename, s3, bucket_name)

    # Load the processed weather data into a Pandas DataFrame
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df = remove_id_column(df)

    # Remove column to forecast, should it exist
    df = df.drop(target_column, axis=1)

    # Load the model from a joblib file
    model_bytes = model_obj['Body'].read()
    model = pickle.loads(model_bytes)

    preds = model.predict(df)
    preds_list = preds.tolist()

    # Serialize the predictions to JSON
    json_data = json.dumps(preds_list)

    bucket_name = bucket_name
    key = "predictions.json"

    #s3.put_object(Body=json_data, Bucket=bucket_name, Key=key)
    write("s3", key, json_data, s3, bucket_name)

# Construct the default_var using the timestamp
default_filename = 'airflow_train'

# Replace bucket_name here with created bucket's name
default_bucket_name = "nithintatikondaawsbucket"

# Load configuration from the Trigger DAG with Config option
config = Variable.get("config", default_var={}, deserialize_json=True)

# Use config values, or fallback to defaults
filename = config.get("filename", default_filename)
bucket_name = config.get("bucket_name", default_bucket_name)
model_path = config.get("model_path", "model.pkl")
target_column = config.get("target_column", "apparent_temperature")

# Set the Airflow Variables
Variable.set("filename", filename)
Variable.set("bucket_name", bucket_name)
Variable.set("model_path", model_path)
Variable.set("target_column", target_column)


def run_get_data(**op_kwargs):
    # Call the get_weather_data function with the filename and bucket_name arguments
    get_weather_data(filename=filename, bucket_name=bucket_name)

def process_data(**op_kwargs):
    # Call the preprocess data function with the filename and bucket_name arguments
    preprocess_data(filename=filename, bucket_name=bucket_name)

def train(**op_kwargs):
    # Call the train model function with the filename, bucket_name, and target_column arguments
    train_model(filename=filename, bucket_name=bucket_name, target_column=target_column)

def predict(**op_kwargs):
    # Call the make predictions function with the model path and bucket_name arguments
    make_predictions(model_path=model_path, bucket_name=bucket_name, filename=filename, target_column=target_column)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    dag_id='ml_pipeline',
    default_args=default_args,
    description='Collecting historical weather data, preprocessing, and forecasting',
    schedule_interval=None
)

run_get_data = PythonOperator(
    task_id="collect_weather_data",
    python_callable=run_get_data,
    provide_context=True,
    op_kwargs={
        'filename': filename, 
        'bucket_name': bucket_name
    },
    dag=dag
)
"""
process_data = PythonOperator(
    task_id='preprocess_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag
)
"""

def read_function():
    # Connect to S3
    s3 = boto3.client('s3')

    filename = f"airflow_train.csv"

    # Get the weather data file from S3
    #obj = s3.get_object(Bucket=bucket_name, Key=filename)
    obj = read("s3", filename, s3, "nithintatikondaawsbucket")
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    return df

def sharding_function(sharding_num, df):
    chunks = np.array_split(df, sharding_num)
    return chunks

def compute_function(df):
    df = remove_id_column(df)

    df_processed = create_lag_window(df)
    df_processed = df_processed.reset_index(drop=True)
    return df_processed

def merge_function(df_list):
    df = pd.concat(df_list)
    return df


def write_function(df):
    s3 = boto3.client('s3')
    bucket_name = "nithintatikondaawsbucket"
    key = f"processed_airflow_train.csv"

    # Write data to a file on S3
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    print(df.dtypes)
    #s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=key)
    write("s3", key, csv_buffer.getvalue(), s3, bucket_name)

process_data = ParallelFusedPythonOperator(
    task_id='preprocess_data',
    data_collection_function=read_function, sharding_function=sharding_function, compute_function=compute_function, merge_function=merge_function, write_function=write_function,
    dag=dag
)

train = PythonOperator(
    task_id='train_model_on_new_data',
    python_callable=train,
    provide_context=True,
    dag=dag
)

predict = PythonOperator(
    task_id='make_predictions_on_processed_data',
    python_callable=predict,
    provide_context=True,
    dag=dag
)

# Create dependency to ensure run_get_data runs first before process_data
run_get_data >> process_data >> train >> predict

fused_dag = create_optimized_dag(dag, timing=True)