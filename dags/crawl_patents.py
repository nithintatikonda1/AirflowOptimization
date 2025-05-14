from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator as DummyOperator

import logging
from datetime import datetime
from typing import List

import requests

import boto3
import os
from airflowfusion.backend_registry import read, write
from airflowfusion.fuse import create_optimized_dag
S3_BUCKET_NAME = os.getenv("S3_BUCKET")


class PatentDto:
    """
    Encapsulate patents information.
    """

    def __init__(self, patent_id: str, patent_nr: str, patent_title: str, patent_date: str, keyword: str):
        self.patent_id = patent_id
        self.patent_nr = patent_nr
        self.patent_title = patent_title
        self.patent_date = patent_date
        self.keyword = keyword

    @staticmethod
    def to_csv_header() -> str:
        return 'patent_id,patent_nr,patent_title,keyword,patent_date'

    def to_csv_entry(self) -> str:
        return f'{self.patent_id},{self.patent_nr},"{self.patent_title}","{self.keyword}",{self.patent_date}'
    
def export_patents(filename: str, patents: List[PatentDto]):
    """
    Export patent DTOs to CSV files.
    :param filename: output filename.
    :param patents: list of patents to serialize.
    """

    with open(filename, 'w', encoding='utf-8') as file:
        file.write(PatentDto.to_csv_header() + '\n')
        for patent in patents:
            file.write(patent.to_csv_entry() + '\n')

PAGES_TO_CRAWL = 10
RESULTS_PER_PAGE = 100
logging.basicConfig(level=logging.INFO)


def crawl(keyword: str):
    """
    Crawl patents based in a keyboard and export the data as CSV in AWS S3.
    :param keyword: Keyword to search for in the patents API.
    """

    patents = list()
    crawled_at = _get_date_today_as_str()
    for i in range(1, PAGES_TO_CRAWL + 1):
        logging.info('Crawling patents for keyboard ' + keyword + ". Page " + str(i))
        url = 'https://api.patentsview.org/patents/query'
        query = 'q={"_or":[{"_text_any":{"patent_title":"' + keyword + '"}}]}'
        query_filter = '&f=["patent_id","patent_number","patent_date","patent_title"]&o={"page":' + \
                       str(PAGES_TO_CRAWL) + ',"per_page":' + str(RESULTS_PER_PAGE) + '}'

        request_uri = url + '?' + query + query_filter
        json = requests.get(request_uri).json()
        print(json)
        patent_list = _map_response_to_object(json['patents'], keyword)
        for patent in patent_list:
            patents.append(patent)

    logging.info('Finished crawling patents for keyboard ' + keyword)

    tmp_csv = 'patents_' + keyword + '_' + crawled_at + '.csv'
    export_patents(tmp_csv, patents)
    #s3.upload_to_aws(tmp_csv, '/patents/' + tmp_csv)
    s3_client = boto3.client('s3')
    s3_client.upload_file(tmp_csv, S3_BUCKET_NAME, '/patents/' + tmp_csv)

def _map_response_to_object(json_list: List[dict], keyword: str) -> List[PatentDto]:
    to_return = list()
    for patent_entry in json_list:
        to_return.append(PatentDto(
            patent_id=patent_entry['patent_id'],
            patent_nr=patent_entry['patent_number'],
            patent_title=patent_entry['patent_title'],
            keyword=keyword,
            patent_date=patent_entry['patent_date'],
        ))
    return to_return

def _get_date_today_as_str() -> str:
    return datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

default_args = {
    'owner': 'juan.roldan@bluggie.com',
    'start_date': datetime(2019, 1, 1),
    'retry_delay': timedelta(minutes=5)
}

with DAG('patent_crawler', default_args=default_args, catchup=False, schedule_interval='0 * * * *') as dag:
    """
    Airflow DAG to crawl patents.
    """

    start_task = DummyOperator(
        task_id='starting_point'
    )

    crawl_phone_patents = PythonOperator(
        task_id='crawl_phone_patents',
        python_callable=crawl,
        op_kwargs={
            'keyword': 'phone'
        },
        dag=dag)

    crawl_software_patents = PythonOperator(
        task_id='crawl_software_patents',
        python_callable=crawl,
        op_kwargs={
            'keyword': 'software'
        },
        dag=dag)

    # Use arrows to set dependencies between tasks
    start_task >> [crawl_phone_patents, crawl_software_patents]

    fused_dag = create_optimized_dag(dag, parallelize=False)
    optimized_dag = create_optimized_dag(dag)