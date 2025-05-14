"""
### Register a Model to MLFlow
Train and Register a Grid Search with MLflow.

Uses a publicly avaliable Census dataset in Bigquery. 

Airflow can integrate with tools like MLFlow to streamline the model experimentation process. 
By using the automation and orchestration of Airflow together with MLflow's core concepts Data Scientists can standardize, share, and iterate over experiments more easily.


#### XCOM Backend
By default, Airflow stores all return values in XCom. However, this can introduce complexity, as users then have to consider the size of data they are returning. Futhermore, since XComs are stored in the Airflow database by default, intermediary data is not easily accessible by external systems.
By using an external XCom backend, users can easily push and pull all intermediary data generated in their DAG in GCS.
"""

from airflow.decorators import task, dag
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflowfusion.operator import ParallelFusedPythonOperator

from datetime import datetime

import lightgbm as lgb

import logging

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from airflowfusion.backend_registry import read, write
from google.cloud import bigquery
import metrics as metrics
import numpy as np
from airflowfusion.fuse import create_optimized_dag


with DAG("register_mlflow", schedule_interval=None, start_date=None) as dag:

    def load_data():
        """Pull Census data from Public BigQuery and save as Pandas dataframe in GCS bucket with XCom"""

        client = bigquery.Client()

        # Define the SQL query
        sql = """
        SELECT * FROM `bigquery-public-data.ml_datasets.census_adult_income`
        """

        # Execute the query and convert the result to a pandas dataframe
        df = client.query(sql).to_dataframe()

        #return bq.get_pandas_df(sql=sql, dialect='standard')
        write('xcom', 'df', df)


    def preprocessing():
        """Clean Data and prepare for feature engineering
        
        Returns pandas dataframe via Xcom to GCS bucket.

        Keyword arguments:
        df -- Raw data pulled from BigQuery to be processed. 
        """
        df = read('xcom', 'df')
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)

        # Clean Categorical Variables (strings)
        cols = df.columns
        for col in cols:
            if df.dtypes[col]=='object':
                df[col] =df[col].apply(lambda x: x.rstrip().lstrip())

        # Rename up '?' values as 'Unknown'
        df['workclass'] = df['workclass'].apply(lambda x: 'Unknown' if x == '?' else x)
        df['occupation'] = df['occupation'].apply(lambda x: 'Unknown' if x == '?' else x)
        df['native_country'] = df['native_country'].apply(lambda x: 'Unknown' if x == '?' else x)

        # Drop Extra/Unused Columns
        df.drop(columns=['education_num', 'relationship', 'functional_weight'], inplace=True)

        #return df
        write('xcom', 'df', df)

    def read_function():
        df = read('xcom', 'df')
        return df
    
    def sharding_function(sharding_num, df):
        chunks = np.array_split(df, sharding_num)
        return chunks

    def compute_function(df):
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)

        # Clean Categorical Variables (strings)
        cols = df.columns
        for col in cols:
            if df.dtypes[col]=='object':
                df[col] =df[col].apply(lambda x: x.rstrip().lstrip())

        # Rename up '?' values as 'Unknown'
        df['workclass'] = df['workclass'].apply(lambda x: 'Unknown' if x == '?' else x)
        df['occupation'] = df['occupation'].apply(lambda x: 'Unknown' if x == '?' else x)
        df['native_country'] = df['native_country'].apply(lambda x: 'Unknown' if x == '?' else x)

        # Drop Extra/Unused Columns
        df.drop(columns=['education_num', 'relationship', 'functional_weight'], inplace=True)

        return df

    def merge_function(df_list):
        df = pd.concat(df_list)
        return df
    
    def write_function(df):
        write('xcom', 'df', df)


    def feature_engineering():
        """Feature engineering step
        
        Returns pandas dataframe via XCom to GCS bucket.

        Keyword arguments:
        df -- data from previous step pulled from BigQuery to be processed. 
        """
        df = read('xcom', 'df')

        # Onehot encoding 
        df = pd.get_dummies(df, prefix='workclass', columns=['workclass'])
        df = pd.get_dummies(df, prefix='education', columns=['education'])
        df = pd.get_dummies(df, prefix='occupation', columns=['occupation'])
        df = pd.get_dummies(df, prefix='race', columns=['race'])
        df = pd.get_dummies(df, prefix='sex', columns=['sex'])
        df = pd.get_dummies(df, prefix='income_bracket', columns=['income_bracket'])
        df = pd.get_dummies(df, prefix='native_country', columns=['native_country'])

        # Bin Ages
        df['age_bins'] = pd.cut(x=df['age'], bins=[16,29,39,49,59,100], labels=[1, 2, 3, 4, 5])

        # Dependent Variable
        df['never_married'] = df['marital_status'].apply(lambda x: 1 if x == 'Never-married' else 0) 

        # Drop redundant column
        df.drop(columns=['income_bracket_<=50K', 'marital_status', 'age'], inplace=True)

        #return df
        write('xcom', 'df', df)


    def grid_search_cv(**kwargs):
        """Train and validate model using a grid search for the optimal parameter values and a five fold cross validation.
        
        Returns accuracy score via XCom to GCS bucket.

        Keyword arguments:
        df -- data from previous step pulled from BigQuery to be processed. 
        """
        df = read('xcom', 'df')


        """
        import mlflow

        mlflow.set_tracking_uri('file:///usr/local/airflow/dags')
        try:
            # Creating an experiment
            mlflow.create_experiment('census_prediction')
        except:
            pass
        # Setting the environment with the created experiment
        mlflow.set_experiment('census_prediction')

        mlflow.sklearn.autolog()
        mlflow.lightgbm.autolog()

        """
        y = df['never_married']
        X = df.drop(columns=['never_married'])

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=55, stratify=y)
        train_set = lgb.Dataset(X_train, label=y_train)
        test_set = lgb.Dataset(X_test, label=y_test)

        params = {'objective': 'binary', 'metric': ['auc', 'binary_logloss']}

        grid_params = {
            'learning_rate': [0.01, .05, .1], 
            'n_estimators': [50, 100, 150],
            'num_leaves': [31, 40, 80],
            'max_depth': [16, 24, 31, 40]
            }

        model = lgb.LGBMClassifier(**params)
        grid_search = GridSearchCV(model, param_grid=grid_params, verbose=1, cv=5, n_jobs=-1)

        #with mlflow.start_run(run_name=f'LGBM {kwargs["run_id"]}'):
        logging.info('Performing Gridsearch')
        grid_search.fit(X_train, y_train)

        logging.info(f'Best Parameters\n{grid_search.best_params_}')
        best_params = grid_search.best_params_
        best_params['metric'] = ['auc', 'binary_logloss']


        logging.info('Training model with best parameters')
        clf = lgb.train(
            train_set=train_set,
            valid_sets=[train_set, test_set],
            valid_names=['train', 'validation'],
            params=best_params
        )

        logging.info('Gathering Validation set results')
        y_pred_class = metrics.test(clf, X_test)


        print(y_pred_class)
        # Log Classfication Report, Confusion Matrix, and ROC Curve
        #metrics.log_all_eval_metrics(y_test, y_pred_class)


    #load_data() >> preprocessing() >> feature_engineering() >> grid_search_cv()
    # Create tasks
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    """
    preprocessing = PythonOperator(
        task_id='preprocessing',
        python_callable=preprocessing,
        provide_context=True,
    ) """
    preprocessing = ParallelFusedPythonOperator(
        task_id='preprocessing',
        data_collection_function=read_function, sharding_function=sharding_function, compute_function=compute_function, merge_function=merge_function, write_function=write_function,
        dag=dag
    )

    feature_engineering = PythonOperator(
        task_id='feature_engineering',
        python_callable=feature_engineering,
        provide_context=True,
    )

    grid_search_cv = PythonOperator(
        task_id='grid_search_cv',
        python_callable=grid_search_cv,
        provide_context=True,
    )

    # Create DAG
    load_data >> preprocessing >> feature_engineering >> grid_search_cv


    fused_dag = create_optimized_dag(dag, parallelize=False)
    optimized_dag = create_optimized_dag(dag)