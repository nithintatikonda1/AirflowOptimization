from airflow.operators.python import PythonOperator
from airflowfusion.read_write_interceptor import ReadWriteInterceptor

class FusedPythonOperator(PythonOperator):
    def __init__(self, *args, **kwargs):
        self.failure_rate = kwargs.get('failure_rate', 0)
        super().__init__(*args, **kwargs)

class ParallelFusedPythonOperator(PythonOperator):
    def __init__(self, *args, **kwargs):
        data_collection_function = kwargs.get('data_collection_function', None)
        sharding_function = kwargs.get('sharding_function', None)
        compute_function = kwargs.get('compute_function', None)
        merge_function = kwargs.get('merge_function', None)
        write_function = kwargs.get('write_function', None)
        kwargs['python_callable'] = lambda : write_function(compute_function(data_collection_function()))

        super().__init__(*args, **kwargs)

        self.data_collection_function = data_collection_function
        self.sharding_function = sharding_function
        self.compute_function = compute_function
        self.merge_function = merge_function
        self.write_function = write_function


        

