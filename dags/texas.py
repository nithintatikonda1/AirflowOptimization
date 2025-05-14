"""
## Play Texas Hold'em Poker with Airflow

This DAG will draw cards for two players (and also show how to use a teardown task
to clean up XComs after the DAG has finished running).

This DAG works with a custom XCom backend and needs:

- the environment variable `XCOM_BACKEND_AWS_CONN_ID` set to `aws_default`
- a connection to S3 with the connection id `aws_default`
- the environment variable `XCOM_BACKEND_BUCKET_NAME` set to the name of an S3 bucket.
- the environment variable `AIRFLOW__CORE__XCOM_BACKEND` set to `include.custom_xcom_backend.s3_xcom_backend.CustomXComBackendS3`
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.utils.dates import days_ago
import json
import requests
from airflowfusion.backend_registry import read, write
from airflowfusion.fuse import create_optimized_dag

def draw_cards(deck_id, number):
    """Draws a number of cards from a deck."""
    cards = []
    for i in range(number):
        r = requests.get(f"https://deckofcardsapi.com/api/deck/{deck_id}/draw/?count=1")
        cards.append(r.json()["cards"][0])
    return cards

def clear_xcom(**kwargs):
    """Clear XCom data at the end of the DAG"""
    task_instance = kwargs['task_instance']
    task_instance.clear_xcom_data()

with DAG(
    dag_id='texas_hold_em',
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    tags=[".as_teardown()", "setup/teardown", "deferrable"],
) as dag:

    # Step 1: Shuffle the deck (dummy HTTP call, just for the sake of this DAG)
    shuffle_cards = HttpOperator(
        task_id="shuffle_cards",
        method="GET",
        http_conn_id="http_default",
        endpoint="api/deck/new/shuffle/?deck_count=1",
    )

    def player_one_draws_cards(**kwargs):
        #shuffle_response = kwargs['ti'].xcom_pull(task_ids='shuffle_cards')
        shuffle_response = read('xcom', 'return_value')
        deck_id = json.loads(shuffle_response)["deck_id"]
        cards = draw_cards(deck_id, 2)
        #kwargs['ti'].xcom_push(key="player_one_cards", value=cards)
        write('xcom', 'player_one_cards', cards)

    def player_two_draws_cards(**kwargs):
        #shuffle_response = kwargs['ti'].xcom_pull(task_ids='shuffle_cards')
        shuffle_response = read('xcom', 'return_value')
        deck_id = json.loads(shuffle_response)["deck_id"]
        cards = draw_cards(deck_id, 2)
        #kwargs['ti'].xcom_push(key="player_two_cards", value=cards)
        write('xcom', 'player_two_cards', cards)

    def cards_on_the_table(**kwargs):
        #shuffle_response = kwargs['ti'].xcom_pull(task_ids='shuffle_cards')
        shuffle_response = read('xcom', 'return_value')
        deck_id = json.loads(shuffle_response)["deck_id"]
        cards = draw_cards(deck_id, 5)
        #kwargs['ti'].xcom_push(key="cards_on_the_table", value=cards)
        write('xcom', 'cards_on_the_table', cards)

    def evaluate_cards(**kwargs):
        ti = kwargs['ti']

        # Pull data from XCom
        #player_one_cards = ti.xcom_pull(task_ids='player_one_draws_cards', key='player_one_cards')
        #player_two_cards = ti.xcom_pull(task_ids='player_two_draws_cards', key='player_two_cards')
        #cards_on_the_table = ti.xcom_pull(task_ids='cards_on_the_table', key='cards_on_the_table')

        player_one_cards = read('xcom', 'player_one_cards')
        player_two_cards = read('xcom', 'player_two_cards')
        cards_on_the_table = read('xcom', 'cards_on_the_table')

        for card in player_one_cards:
            print(f"Player 1 drew: {card['value']} of {card['suit']}")
        for card in player_two_cards:
            print(f"Player 2 drew: {card['value']} of {card['suit']}")
        for card in cards_on_the_table:
            print(f"On the table we have a: {card['value']} of {card['suit']}")

    # Task Definitions
    start = EmptyOperator(task_id="start")

    player_one_draws = PythonOperator(
        task_id="player_one_draws_cards",
        python_callable=player_one_draws_cards,
    )

    player_two_draws = PythonOperator(
        task_id="player_two_draws_cards",
        python_callable=player_two_draws_cards,
    )

    draw_cards_table = PythonOperator(
        task_id="cards_on_the_table",
        python_callable=cards_on_the_table,
    )

    evaluate = PythonOperator(
        task_id="evaluate_cards",
        python_callable=evaluate_cards,
    )

    # Task Dependencies
    start >> shuffle_cards
    shuffle_cards >> player_one_draws >> player_two_draws >> draw_cards_table >> evaluate

    # XCom Cleanup
    cleanup_xcom = PythonOperator(
        task_id="cleanup_xcom",
        python_callable=clear_xcom,
        provide_context=True,
    )

    evaluate >> cleanup_xcom

    fused_dag = create_optimized_dag(dag, parallelize=False)
    optimized_dag = create_optimized_dag(dag)