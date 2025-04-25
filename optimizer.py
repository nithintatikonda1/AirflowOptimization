import os
import sys
import logging
from pathlib import Path

# Set up logging to track the progress of script execution
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def execute_dag_files_in_folder(dag_folder_path):
    """
    Executes all Python DAG files in the specified DAG folder.
    The current working directory will be set to the DAG folder before each file is executed.
    """
    # Get the list of all Python files in the DAG folder
    dag_folder = Path(dag_folder_path)
    
    if not dag_folder.is_dir():
        logger.error(f"The provided path '{dag_folder_path}' is not a valid directory.")
        return
    
    # Get all .py files in the DAG folder (excluding directories)
    dag_files = [file for file in dag_folder.glob('*.py') if file.is_file()]
    
    # Check if there are any Python files to execute
    if not dag_files:
        logger.warning(f"No Python DAG files found in the directory '{dag_folder_path}'.")
        return

    # Loop through all the .py files and execute them
    for dag_file in dag_files:
        if not str(dag_file).endswith('aws_change.py'):
            continue
        logger.info(f"Executing DAG file: {dag_file}")
        
        # Change the current working directory to the DAG folder
        original_cwd = os.getcwd()
        os.chdir(dag_folder)
        
        try:
            # Use exec() to execute the DAG file in the context of the current working directory
            with open(dag_file) as file:
                exec(file.read())
            logger.info(f"Successfully executed {dag_file}")
        except Exception as e:
            logger.error(f"Error executing {dag_file}: {e}")
        finally:
            # Restore the original working directory
            os.chdir(original_cwd)

if __name__ == "__main__":
    # Set the path to the DAG folder
    dag_folder_path = os.path.join(os.getcwd(), 'dags')  # Adjust if needed

    # Execute all the DAG files
    execute_dag_files_in_folder(dag_folder_path)