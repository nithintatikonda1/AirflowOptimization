from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from pprint import pprint
from airflowfusion.fuse import create_optimized_dag_integer_programming

def checkPrice(**kwargs):
    import math
    import random

    # generate a random int between 2 values
    def getRandomInt(min, max):
        min = math.ceil(min)
        max = math.floor(max)
        return math.floor(random.random() * (max - min) + min)

    # simulate a stock price with a random number
    price = getRandomInt(0, 100)
    print(f"Stock price is: {str(price)}")
    ti = kwargs['ti']
    ti.xcom_push(key='checkPrice', value = price)

def buy_sell_recommendation(**kwargs):

    # determine if we should buy or sell based on price
    ti = kwargs['ti']
    _price = ti.xcom_pull(key='checkPrice')

    ti.xcom_push(key='buy_sell_recommendation', value = "sell" if _price > 50 else "buy")

def human_approval():
    pass

def buy_or_sell(**kwargs):
    ti = kwargs['ti']
    rec = ti.xcom_pull(key='buy_sell_recommendation')
    if rec == "buy":
        return ['buy']
    return ['sell']

def buy(**kwargs):
    import json
    import datetime
    import math
    import random
    import uuid
    # generate a random int between 2 values
    def getRandomInt(min, max):
        min = math.ceil(min)
        max = math.floor(max)
        return math.floor(random.random() * (max - min) + min)
    
    ti = kwargs['ti']
    _price = ti.xcom_pull(key='checkPrice')

    print(f"Buying for ${_price}")
    _now = datetime.datetime.now()

    transaction_result = {
        "price": _price,
        "transaction_id": str(uuid.uuid1),
        "type": "buy",
        "qty": getRandomInt(1, 100),
        "timestamp": str(_now),
    }

    return transaction_result

def sell(**kwargs):
    import json
    import datetime
    import math
    import random
    import uuid

    # generate a random int between 2 values
    def getRandomInt(min, max):
        min = math.ceil(min)
        max = math.floor(max)
        return math.floor(random.random() * (max - min) + min)

    ti = kwargs['ti']
    _price = ti.xcom_pull(key='checkPrice')

    print(f"Selling for ${_price}")
    _now = datetime.datetime.now()

    transaction_result = {
        "transaction_id": str(uuid.uuid1),
        "price": _price,
        "type": "sell",
        "qty": getRandomInt(1, 100),
        "timestamp": str(_now),
    }

    return transaction_result

dag = DAG(
    dag_id='stock',
    description='Buy or sell stock',
    schedule_interval=None
)


t1 = PythonOperator(
    task_id="checkPrice",
    python_callable=checkPrice,
    dag=dag,
    provide_context=True,
)

t2 = PythonOperator(
    task_id="buy_sell_recommendation",
    python_callable=buy_sell_recommendation,
    dag=dag,
    provide_context=True,
)

t3 = PythonOperator(
    task_id="human_approval",
    python_callable=human_approval,
    dag=dag,
    provide_context=True,
)

branching = BranchPythonOperator(
    task_id='buy_or_sell',
    python_callable=buy_or_sell,
)

t4 = PythonOperator(
    task_id="buy",
    python_callable=buy,
    dag=dag,
    provide_context=True,
)

t5 = PythonOperator(
    task_id="sell",
    python_callable=sell,
    dag=dag,
    provide_context=True,
)

t1 >> t2 >> t3 >> branching
branching >> t4
branching >> t5

total_costs = {'checkPrice': 2, 'buy_sell_recommendation': 2, 'human_approval': 2, 'buy_or_sell': 2, 'buy': 2, 'sell': 2}
read_costs = {
    'checkPrice': {'checkPrice': 1},
    'buy_sell_recommendation': {'buy_sell_recommendation': 1},
}

fused_dag = create_optimized_dag_integer_programming(dag, total_costs, read_costs, 1)