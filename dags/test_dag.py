from airflow import DAG
from airflow.operators.python import PythonOperator
from pprint import pprint
from airflow.decorators import task



with DAG(dag_id="test_parallel2") as dag:

    def start():
        print("Starting")

    def add_one():
        for i in range(10000000000 // 40):
            x = 1
            x += 1
    
    def end():
        print("Ending")
    
    s = PythonOperator(task_id="start", python_callable=start)
    e = PythonOperator(task_id="end", python_callable=end)
    
    m1 = PythonOperator(task_id="add_one1", python_callable=add_one)

    #Make the taks ids unique
    m2 = PythonOperator(task_id="add_one2", python_callable=add_one)
    m3 = PythonOperator(task_id="add_one3", python_callable=add_one)
    m4 = PythonOperator(task_id="add_one4", python_callable=add_one)
    m5 = PythonOperator(task_id="add_one5", python_callable=add_one)
    m6 = PythonOperator(task_id="add_one6", python_callable=add_one)
    m7 = PythonOperator(task_id="add_one7", python_callable=add_one)
    m8 = PythonOperator(task_id="add_one8", python_callable=add_one)
    m9 = PythonOperator(task_id="add_one9", python_callable=add_one)
    m10 = PythonOperator(task_id="add_one10", python_callable=add_one)
    m11 = PythonOperator(task_id="add_one11", python_callable=add_one)
    m12 = PythonOperator(task_id="add_one12", python_callable=add_one)
    m13 = PythonOperator(task_id="add_one13", python_callable=add_one)
    m14 = PythonOperator(task_id="add_one14", python_callable=add_one)
    m15 = PythonOperator(task_id="add_one15", python_callable=add_one)
    m16 = PythonOperator(task_id="add_one16", python_callable=add_one)
    m17 = PythonOperator(task_id="add_one17", python_callable=add_one)
    m18 = PythonOperator(task_id="add_one18", python_callable=add_one)
    m19 = PythonOperator(task_id="add_one19", python_callable=add_one)
    m20 = PythonOperator(task_id="add_one20", python_callable=add_one)
    m21 = PythonOperator(task_id="add_one21", python_callable=add_one)    
    m22 = PythonOperator(task_id="add_one22", python_callable=add_one)
    m23 = PythonOperator(task_id="add_one23", python_callable=add_one)
    m24 = PythonOperator(task_id="add_one24", python_callable=add_one)
    m25 = PythonOperator(task_id="add_one25", python_callable=add_one)
    m26 = PythonOperator(task_id="add_one26", python_callable=add_one)
    m27 = PythonOperator(task_id="add_one27", python_callable=add_one)
    m28 = PythonOperator(task_id="add_one28", python_callable=add_one)
    m29 = PythonOperator(task_id="add_one29", python_callable=add_one)
    m30 = PythonOperator(task_id="add_one30", python_callable=add_one)
    m31 = PythonOperator(task_id="add_one31", python_callable=add_one)
    m32 = PythonOperator(task_id="add_one32", python_callable=add_one)
    m33 = PythonOperator(task_id="add_one33", python_callable=add_one)
    m34 = PythonOperator(task_id="add_one34", python_callable=add_one)
    m35 = PythonOperator(task_id="add_one35", python_callable=add_one)    
    m36 = PythonOperator(task_id="add_one36", python_callable=add_one)
    m37 = PythonOperator(task_id="add_one37", python_callable=add_one)
    m38 = PythonOperator(task_id="add_one38", python_callable=add_one)
    m39 = PythonOperator(task_id="add_one39", python_callable=add_one)
    m40 = PythonOperator(task_id="add_one40", python_callable=add_one)        

    s >> m1
    m1 >> e
    s >> m2 >> e
    s >> m3 >> e
    s >> m4 >> e
    s >> m5 >> e
    s >> m6 >> e
    s >> m7 >> e
    s >> m8 >> e
    s >> m9 >> e
    s >> m10 >> e
    s >> m11 >> e    
    s >> m12 >> e    
    s >> m13 >> e    
    s >> m14 >> e    
    s >> m15 >> e    
    s >> m16 >> e    
    s >> m17 >> e    
    s >> m18 >> e    
    s >> m19 >> e    
    s >> m20 >> e    
    s >> m21 >> e    
    s >> m22 >> e    
    s >> m23 >> e    
    s >> m24 >> e    
    s >> m25 >> e    
    s >> m26 >> e    
    s >> m27 >> e    
    s >> m28 >> e    
    s >> m29 >> e    
    s >> m30 >> e    
    s >> m31 >> e    
    s >> m32 >> e    
    s >> m33 >> e    
    s >> m34 >> e    
    s >> m35 >> e    
    s >> m36 >> e    
    s >> m37 >> e    
    s >> m38 >> e        
    s >> m39 >> e        
    s >> m40 >> e





