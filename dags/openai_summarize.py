"""
## Summarize and search financial documents using OpenAI's LLMs.

This DAG extracts and splits financial reporting data from the US 
[Securities and Exchanges Commision (SEC) EDGAR database](https://www.sec.gov/edgar) and generates 
vector embeddings with OpenAI embeddings model for generative question answering.  The DAG also 
creates and vectorizes summarizations of the 10-Q document using OpenAI completions.
"""
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.providers.openai.hooks.openai import OpenAIHook
from airflowfusion.backend_registry import read, write
import openai

from bs4 import BeautifulSoup
import datetime
from langchain.schema import Document
from langchain.text_splitter import (
    HTMLHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)
import logging
import openai as openai_client
import pandas as pd
from pathlib import Path
import requests
import unicodedata
import uuid
import os
import numpy as np
from airflowfusion.backend_registry import read, write
from airflowfusion.fuse import create_optimized_dag

from airflowfusion.operator import ParallelFusedPythonOperator

OPENAI_CONN_ID = "openai_default"

API_KEY = os.environ.get("OPENAI_KEY")

logger = logging.getLogger("airflow.task")

edgar_headers={"User-Agent": "test1@test1.com"}



@dag(
    schedule_interval=None,
    start_date=datetime.datetime(2023, 9, 27)
)
def FinSum_OpenAI():
    """
    This DAG extracts and splits financial reporting data from the US 
    [Securities and Exchanges Commision (SEC) EDGAR database](https://www.sec.gov/edgar) and generates 
    vector embeddings with openai embeddings model for generative question answering.  The DAG also 
    creates and vectorizes summarizations of the 10-Q document.

    With very large datasets it may not be convenient to store embeddings in a vector database.  This DAG
    shows how to save documents with vectors on disk. Realistically these would be serialized in cloud object 
    storage but for the purpose of demo we store them on local disk.

    """
    ticker = "NFLX"

    def remove_html_tables(content:str):
        """
        Remove all "table" tags from html content leaving only text.

        :param content: html content
        :return: A string of extracted text from html without tables.
        """
        soup = BeautifulSoup(content, "lxml")

        for table in soup.find_all("table"):
            _ = table.replace_with(" ")
        soup.smooth()
        
        clean_text = unicodedata.normalize("NFKD", soup.text)

        return clean_text

    def get_html_content(doc_link: str) -> str:
        """
        A helper function to support pandas apply. Scrapes doc_link for html content.

        :param doc_link: Page url
        :return: Extracted plain text from html without any tables.
        """
        content = requests.get(doc_link, headers=edgar_headers)
        
        if content.ok:
            content_type = content.headers['Content-Type']
            if content_type == 'text/html':
                content = remove_html_tables(content.text)
            else:
                logger.warning(f"Unsupported content type ({content_type}) for doc {doc_link}.  Skipping.")
                content = None
        else:
            logger.warning(f"Unable to get content.  Skipping. Reason: {content.status_code} {content.reason}")
            content = None
        
        return content

    def get_10q_link(accn: str, cik_number: str) -> str:
        """
        Given an Accn number from SEC filings index, returns the URL of the 10-Q document.

        :param accn: account number for the filing
        :param cik_number: SEC Central Index Key for the company
        :return: Fully-qualified url pointing to a 10-Q filing document.
        """
        
        url_base = f"https://www.sec.gov/Archives/edgar/data/"

        link_base = f"{url_base}{cik_number}/{accn.replace('-','')}/"

        filing_summary = requests.get(f"{link_base}{accn}-index.html", headers=edgar_headers)

        link = None
        if filing_summary.ok:

            soup = BeautifulSoup(filing_summary.content, "lxml")

            for tr in soup.find("table", {"class": "tableFile"}).find_all("tr"):
                for td in tr.find_all('td'):
                    if td.text == "10-Q":
                        link = link_base + tr.find('a').text
        else:
            logger.warn(f"Error extracting accn index. Reason: {filing_summary.status_code} {filing_summary.reason}")

        return link

    def extract() -> pd.DataFrame:
        """
        This task pulls 10-Q statements from the [SEC Edgar database](https://www.sec.gov/edgar/searchedgar/companysearch)

        :param ticker: ticker symbol of company 
        :param cik_number: optionally cik_number instead of ticker symbol
        :return: A dataframe
        """
        ticker = "NFLX"

        logger.info(f"Extracting documents for ticker {ticker}.")

        company_list = requests.get(
            url="https://www.sec.gov/files/company_tickers.json", 
            headers=edgar_headers)

        if company_list.ok:
            company_list = list(company_list.json().values())
            cik_numbers = [item for item in company_list if item.get("ticker") == ticker.upper()]

            if len(cik_numbers) != 1:
                raise ValueError("Provided ticker symbol is not available.")
            else:
                cik_number = str(cik_numbers[0]['cik_str'])

        else:
            logger.error("Could not access ticker database.")
            logger.error(f"Reason: {company_list.status_code} {company_list.reason}")
            raise AirflowException("Could not access ticker database.")
        
        company_facts = requests.get(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_number.zfill(10)}.json", 
            headers=edgar_headers
            )
            
        if company_facts.ok:
            forms_10q = []
            for fact in company_facts.json()['facts']['us-gaap'].values():
                for currency, units in fact['units'].items():
                    for unit in units:
                        if unit["form"] == "10-Q":
                            forms_10q.append(unit)

            forms_10q = pd.DataFrame(forms_10q)[["accn", "fy", "fp"]].drop_duplicates().to_dict('records')

        else:
            logger.error(f"Could not get company filing information for ticker: {ticker}, cik: {cik_number}.")
            logger.error(f"Reason: {company_facts.status_code} {company_facts.reason}")
            raise AirflowException(f"Could not get company filing information for ticker: {ticker}, cik: {cik_number}.")
            
        docs = []
        for form in forms_10q:
            link_10q = get_10q_link(accn=form.get("accn"), cik_number=cik_number)
            docs.append({
                "docLink": link_10q, 
                "tickerSymbol": ticker,
                "cikNumber": cik_number,
                "fiscalYear": form.get("fy"), 
                "fiscalPeriod": form.get("fp")
                })
            
        df = pd.DataFrame(docs)

        df["content"] = df.docLink.apply(lambda x: get_html_content(doc_link=x))
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        write('xcom', 'df', df)

    def split() -> pd.DataFrame:
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and splits the content 
        first with an html splitter and then with a text splitter.

        :param df: A dataframe from an upstream extract task
        :return: A dataframe 
        """
        df = read('xcom', 'df')
        headers_to_split_on = [
            ("h2", "h2"),
        ]

        html_splitter = HTMLHeaderTextSplitter(headers_to_split_on)
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=10000, chunk_overlap=200, separators=["\n\n", "\n", " ", ""]
            )

        df["doc_chunks"] = df["content"].apply(lambda x: html_splitter.split_text(text=x))
        df = df.explode("doc_chunks", ignore_index=True)
        df["content"] = df["doc_chunks"].apply(lambda x: x.page_content)

        df["doc_chunks"] = df["content"].apply(
            lambda x: text_splitter.split_documents([Document(page_content=x)])
            )
        df = df.explode("doc_chunks", ignore_index=True)
        df["content"] = df["doc_chunks"].apply(lambda x: x.page_content)

        df.drop(["doc_chunks"], inplace=True, axis=1)
        df.drop_duplicates(subset=["docLink", "content"], keep="first", inplace=True)
        df.reset_index(inplace=True, drop=True)

        write('xcom', 'df', df)

    def vectorize() -> str:
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and 
        vectorizes with OpenAI Embeddings.

        :param df: A Pandas dataframes from upstream split tasks
        :param content_column_name: The name of the column with text to embed and ingest
        :param output_file_name: Path for saving embeddings as a parquet file
        :return: Location of saved file
        """
        df = read('xcom', 'df')
        output_file_name='include/data/html/openai_embeddings.parquet',
        content_column_name="content"
        
        openai_hook = OpenAIHook(OPENAI_CONN_ID)

        df["id"] = df[content_column_name].apply(
            lambda x: str(uuid.uuid5(
                name=x, 
                namespace=uuid.NAMESPACE_DNS)))

        df["vector"] = df[content_column_name].apply(
            lambda x: openai_hook.create_embeddings(
                text=x, 
                model="text-embedding-ada-002")
            )
        

    def vectorize2() -> str:
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and 
        vectorizes with OpenAI Embeddings.

        :param df: A Pandas dataframes from upstream split tasks
        :param content_column_name: The name of the column with text to embed and ingest
        :param output_file_name: Path for saving embeddings as a parquet file
        :return: Location of saved file
        """
        output_file_name='include/data/html/openai_summary_embeddings.parquet'
        content_column_name= "summary"
        df = read('xcom', 'summary')
        
        openai_hook = OpenAIHook(OPENAI_CONN_ID)

        df["id"] = df[content_column_name].apply(
            lambda x: str(uuid.uuid5(
                name=x, 
                namespace=uuid.NAMESPACE_DNS)))

        df["vector"] = df[content_column_name].apply(
            lambda x: openai_hook.create_embeddings(
                text=x, 
                model="text-embedding-ada-002")
            )

    def chunk_summarization_openai(
            openai_client: openai_client, content: str, ticker: str, fy: str, fp: str) -> str:
        """
        This function uses openai gpt-3.5-turbo-1106 to summarize a chunk of text.

        :param content: The text content to be summarized.
        :param ticker: The company ticker symbol for (status printing).
        :param fy: The fiscal year of the document chunk for (status printing).
        :param fp: The fiscal period of the document chunk for (status printing).
        :return: A summary string
        """

        logger.info(f"Summarizing chunk for ticker {ticker} {fy}:{fp}")
        
        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=[
                {
                    "role": "system",
                    "content": "You are a highly skilled AI trained in language comprehension and summarization. I would like you to read the following text and summarize it into a concise abstract paragraph. Aim to retain the most important points, providing a coherent and readable summary that could help a person understand the main points of the discussion without needing to read the entire text. Please avoid unnecessary details or tangential points."
                },
                {
                    "role": "user",
                    "content": content
                }
            ],
            temperature=0,
            max_tokens=1000
            )
        if content:=response.choices[0].message.content:
            return content
        else:
            return None
    
    def doc_summarization_openai(
            openai_client: openai_client, content: str, doc_link: str) -> str:
        """
        This function uses openai gpt-4-1106-preview to summarize a concatenation of
        document chunks.

        :param content: The text content to be summarized.
        :param doc_link: The URL of the document being summarized (status printing).
        :return: A summary string
        """

        logger.info(f"Summarizing document for {doc_link}")

        response = openai_client.chat.completions.create(
            model="gpt-4-1106-preview",
            messages=[
                {
                    "role": "system",
                    "content": "You are a highly skilled AI trained in language comprehension and summarization. I would like you to read the following text and summarize it into a concise abstract paragraph. Aim to retain the most important points, providing a coherent and readable summary that could help a person understand the main points of the discussion without needing to read the entire text. Please avoid unnecessary details or tangential points."
                },
                {
                    "role": "user",
                    "content": content
                }
            ],
            temperature=0,
            max_tokens=1000
            )
        if content:=response.choices[0].message.content:
            return content
        else:
            return None
        
    def summarize_openai() -> pd.DataFrame:
        """
        This task uses openai to recursively summarize extracted documents. First the individual
        chunks of the document are summarized.  Then the collection of chunk summaries are summarized.

        :param df: A Pandas dataframe from upstream split tasks
        :return: A Pandas dataframe with summaries for ingest to a vector DB.
        """
        df = read('xcom', 'df')
        
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

        write('xcom', 'summary', summary_df)

    def read_function1():
        df = read('xcom', 'df')
        return df
    
    def sharding_function1(sharding_num, df):
        #split df into sharding num chunks
        chunks = np.array_split(df, sharding_num)
        return chunks
    
    def compute_function1(df):
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
    
    def merge_function1(df_list):
        df = pd.concat(df_list)
        return df
    
    def write_function1(df):
        write('xcom', 'summary', df)

    edgar_docs = task(extract)()

    split_docs = task(split)()

    edgar_docs >> split_docs

    embeddings_file = task(vectorize, task_id="vectorize_chunks")()

    split_docs >> embeddings_file

    #generate_summary = task(summarize_openai)()
    generate_summary = ParallelFusedPythonOperator(task_id='summarize', data_collection_function=read_function1, sharding_function=sharding_function1, compute_function=compute_function1, merge_function=merge_function1, write_function=write_function1)

    split_docs >> generate_summary

    summaries_file = task(vectorize2, task_id="vectorize_summaries")()

    generate_summary>> summaries_file

dag = FinSum_OpenAI()
fused_dag = create_optimized_dag(dag, timing=True)