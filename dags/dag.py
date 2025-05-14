from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from textblob import TextBlob

# Data Ingestion Task
def data_ingestion(**kwargs):
    # Example: Fetch data from an external API (or any other source)
    data = {"tweets": ["I love AI!", "AI is great", "I hate AI!", "AI is transforming the world"]}
    # Here, we mock up some data for the sake of the example
    df = pd.DataFrame(data)
    print("Data Ingestion Successful!")
    kwargs['ti'].xcom_push(key='tweets_data', value=df)  # Push data to XCom for use by other tasks
    return df

# Data Summarization Task
def data_summary(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='data_ingestion', key='tweets_data')  # Get data from XCom
    # Generate a basic summary, you can extend this to more complex summaries
    summary = df['tweets'].apply(lambda tweet: len(tweet.split()))  # Example: Count words in each tweet
    print(f"Summary: {summary}")
    kwargs['ti'].xcom_push(key='summary_data', value=summary)  # Push the summary to XCom
    return summary

# Sentiment Analysis Task
def sentiment_analysis(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='data_ingestion', key='tweets_data')  # Get data from XCom
    # Use TextBlob for sentiment analysis
    df['sentiment'] = df['tweets'].apply(lambda tweet: TextBlob(tweet).sentiment.polarity)
    print("Sentiment Analysis Completed!")
    kwargs['ti'].xcom_push(key='sentiment_data', value=df)  # Push the result to XCom
    return df
def sa_read():
        df = read('xcom', 'df')
        return df
    
def sa_shard(sharding_num, df):
    #split df into sharding num chunks
    chunks = np.array_split(df, sharding_num)
    return chunks

def sa_compute(df):
    client = openai.OpenAI(api_key=API_KEY)

    df["chunk_summary"] = df.apply(lambda x: chunk_summarization_openai(
        openai_client=client, 
        content=x.content, 
        fy=x.fiscalYear, 
        fp=x.fiscalPeriod, 
        ticker=x.tickerSymbol), axis=1)

    summaries_df = df.groupby("docLink").chunk_summary.apply("\n".join).reset_index()

    summaries_df["summary"] = summaries_df.apply(lambda x: doc_summarization_openai(
        openai_client=client, 
        content=x.chunk_summary, 
        doc_link=x.docLink), axis=1)
    
    summaries_df.drop("chunk_summary", axis=1, inplace=True)

    summary_df = df.drop(["content", "chunk_summary"], axis=1).drop_duplicates().merge(summaries_df)

    return summary_df

def sa_merge(df_list):
    df = pd.concat(df_list)
    return df

def sa_write(df):
    write('xcom', 'summary', df)


# Email Summary Task
def email_summary(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='data_summary', key='summary_data')  # Get summary data from XCom
    sentiment_df = kwargs['ti'].xcom_pull(task_ids='sentiment_analysis', key='sentiment_data')  # Get sentiment data from XCom
    
    # Example of combining the summary and sentiment information
    print(f"Email Summary: Data Summary: {df}, Sentiment: {sentiment_df}")
    # Here you can trigger an email notification or any other action based on this summary

def data_ingestion():
    data = ingest_data()
    write('xcom', 'data_ingestion', data)

def data_summary():
    data = read('xcom', 'data_ingestion', data)
    summary = summarize_data(data)
    write('xcom', 'data_summary', summary)

def sa_read():
    data = read('xcom', 'data_ingestion')
    return data

def sa_shard(sharding_num, df):
    chunks = np.array_split(df, sharding_num)
    return chunks

def sa_compute(df):
    df = compute(df)
    return df

def sa_merge(df_list):
    df = pd.concat(df_list)
    return df

def sa_write(df):
    write('xcom', 'sentiment_analysis', df)

def email_summary():
    summary = read('xcom', 'data_summary')
    email_summary_to_subscribers(summary)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1),
}

with DAG(dag_id='tweet_analysis_dag',
         default_args=default_args) as dag:

    # Declare Tasks
    t1 = PythonOperator(task_id='data_ingestion', python_callable=data_ingestion)
    t2 = PythonOperator(task_id='data_summary', python_callable=data_summary)
    t3 = ParallelFusedPythonOperator(task_id='sentiment_analysis', 
        data_collection_function=sa_read, 
        sharding_function=sa_shard, 
        compute_function=sa_compute, 
        merge_function=sa_merge, 
        write_function=sa_write
    )
    t4 = PythonOperator(task_id='email_summary', python_callable=email_summary)

    # Task Dependencies
    t1 >> [t2, t3]
    t2 >> t4
    t3 >> t4