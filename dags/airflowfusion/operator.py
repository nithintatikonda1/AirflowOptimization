from airflow.operators.python import PythonOperator
from airflowfusion.read_write_interceptor import ReadWriteInterceptor
from airflow.models.baseoperator import BaseOperator

class FusedPythonOperator(BaseOperator):
    def __init__(self, execute_function, **kwargs):
        super().__init__(**kwargs)
        self.execute_function = execute_function

    def execute(self, context):
        return self.execute_function(context)



class ParallelFusedPythonOperator(BaseOperator):
    def __init__(self, data_collection_function, sharding_function, compute_function, merge_function, write_function, **kwargs):
        super().__init__(**kwargs)

        self.data_collection_function = data_collection_function
        self.sharding_function = sharding_function
        self.compute_function = compute_function
        self.merge_function = merge_function
        self.write_function = write_function


    def execute(self, context):
        return self.write_function(self.compute_function(self.data_collection_function()))

