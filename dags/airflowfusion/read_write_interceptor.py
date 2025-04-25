from functools import wraps
from airflowfusion.backend_registry import read, write

class ReadWriteInterceptor:
    def __init__(self):
        self.cache = {}  # Stores cached values
        self.original_read = read
        self.original_write = write
    
    def cached_read(self, backend_name, key, *args, **kwargs):
        """Checks cache before performing a backend read."""
        print(f"Attempting to read from cache: {backend_name}, {key}")
        if (backend_name, key) in self.cache:
            return self.cache[(backend_name, key)]
        
        value = self.original_read(backend_name, key, *args, **kwargs)
        self.cache[(backend_name, key)] = value  # Cache the result
        return value
    
    def cached_write(self, backend_name, key, value, *args, **kwargs):
        """Performs a backend write and updates cache."""
        self.cache[(backend_name, key)] = value  # Store in cache
        self.original_write(backend_name, key, value, *args, **kwargs)
    
    def optimize_function(self, func):
        """Wraps a function to use optimized reads/writes."""
        @wraps(func)
        def wrapper(context):
            func_globals = func.__globals__
            original_read = func_globals.get("read")
            original_write = func_globals.get("write")

            try:
                func_globals["read"] = self.cached_read
                func_globals["write"] = self.cached_write
                return func(context)
            finally:
                func_globals["read"] = original_read
                func_globals["write"] = original_write

        return wrapper
    
    def optimize_function_without_context(self, func):
        @wraps(func)
        def wrapper():
            func_globals = func.__globals__
            original_read = func_globals.get("read")
            original_write = func_globals.get("write")

            try:
                func_globals["read"] = self.cached_read
                func_globals["write"] = self.cached_write
                return func()
            finally:
                func_globals["read"] = original_read
                func_globals["write"] = original_write

        return wrapper
    
    def get_pipeline_function(self, operators):
        """Returns a function that executes a list of functions in sequence with optimized reads/writes."""

        def pipeline_function(context):
            print("Executing pipeline function")
            for operator in operators:
                optimized_func = self.optimize_function(operator.execute)
                optimized_func(context)

        return pipeline_function
    
