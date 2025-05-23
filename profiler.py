import subprocess
import time
import docker
import os
import json
import psycopg2
from pprint import pprint
import pickle
import tarfile
import io

# ----------- CONFIGURATIONS --------------
DAGS_TO_RUN = ['aws_change', 'bedrock_blog_generator', 'patent_crawler', "simple_redshift_1", "gx_pandas", "find_the_iss", 'FinSum_OpenAI',
               "push_pull", "register_mlflow", 's3_upload', 's3_upload_copy', 'manatee_sentiment', 'stock', 'telephone_game_1', 'texas_hold_em',
               'text_processing','ml_pipeline'] 
#DAGS_TO_RUN = DAGS_TO_RUN[5:]
DAGS_TO_RUN = ['etl_food_pipeline']
LOG_FILE_TEMPLATE = "/usr/local/airflow/dags/{}/log.txt"  # Path inside scheduler container
OUTPUT_DIR = "./include/dag_timings"  # Directory to store parsed results

# default Postgres connection details
POSTGRES_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432,
}

# Load POSTGRES_CONFIG from environment variables
POSTGRES_CONFIG['dbname'] = os.environ.get('POSTGRES_DB', POSTGRES_CONFIG['dbname'])
POSTGRES_CONFIG['user'] = os.environ.get('POSTGRES_USER', POSTGRES_CONFIG['user'])
POSTGRES_CONFIG['password'] = os.environ.get('POSTGRES_PASSWORD', POSTGRES_CONFIG['password'])
POSTGRES_CONFIG['host'] = os.environ.get('POSTGRES_HOST', POSTGRES_CONFIG['host'])
POSTGRES_CONFIG['port'] = os.environ.get('POSTGRES_PORT', POSTGRES_CONFIG['port'])

# ----------- FUNCTION DEFINITIONS -------------

def start_airflow():
    """Starts Airflow using Astronomer CLI."""
    print("Starting Airflow...")
    try:
        subprocess.run(["astro", "dev", "start"], check=True)
        time.sleep(30)  # Give some time for Airflow to fully start
    except subprocess.CalledProcessError as e:
        print("Already running Airflow!")

def trigger_dag(dag_id):
    """Triggers a single DAG run."""
    print(f"Triggering DAG: {dag_id}")
    subprocess.run(["astro", "dev", "run", "dags", "trigger", dag_id], check=True)

def get_container_by_name(name_filter):
    """Finds a running container by name filter."""
    client = docker.from_env()
    containers = client.containers.list(filters={"name": name_filter})
    if not containers:
        raise RuntimeError(f"Container with filter '{name_filter}' not found!")
    return containers[0]

def initialize_log_file(container, dag_id):
    """Ensures log file exists and is cleared before execution."""
    log_path = LOG_FILE_TEMPLATE.format(dag_id)
    folder_path = log_path[:-8]
    print(f"Creating directory: {folder_path} and Initializing log file: {log_path}")

    # Command to clear or create the file
    #cmd = f"bash -c 'mkdir -p $(dirname {log_path}) && > {log_path}'"
    cmd1 = f"mkdir -p {folder_path}"
    cmd2 = f"rm {log_path}"
    cmd3 = f"touch {log_path}"
    
    # Run command inside scheduler container
    exit_code, output = container.exec_run(cmd1)
    time.sleep(5)
    exit_code, output = container.exec_run(cmd2)
    time.sleep(5)
    exit_code, output = container.exec_run(cmd3)
    time.sleep(5)

    if exit_code != 0:
        raise RuntimeError(f"Failed to initialize log file: {output}")

def fetch_log_file(container, dag_id):
    """Retrieves log file for specific DAG from scheduler container."""
    log_path = LOG_FILE_TEMPLATE.format(dag_id)
    print(f"Fetching log file for DAG '{dag_id}' from path: {log_path}")
    try:
        stream, _ = container.get_archive(log_path)
        tar_bytes = b"".join(stream)
        tar_file = tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:")

        # Extract the actual file content
        for member in tar_file.getmembers():
            file = tar_file.extractfile(member)  # Get the file object
            if file:
                log_content = file.read().decode("utf-8")
                return log_content
    except Exception as e:
        print(f"Failed to fetch log file for DAG '{dag_id}': {e}")
        return ""

def parse_log_data(log_text):
    """Parses the timing log file into a dictionary."""
    timing_dict = {}
    for line in log_text.strip().split("\n"):
        parts = line.strip().split()
        if len(parts) != 5:
            continue
        read_write, task_id, backend_name, key, duration = parts
        key = key[1:-1]
        duration = float(duration)
        if task_id not in timing_dict:
            timing_dict[task_id] = {}
        timing_dict[task_id][(backend_name, key, read_write)] = duration
    return timing_dict

def get_task_durations_from_db(dag_id, pg_conn):
    """Fetches total duration of each task from Airflow metadata database."""
    print(f"Querying durations for DAG '{dag_id}'")
    query = """
        SELECT task_id, AVG(duration) as total_duration
        FROM task_instance
        WHERE dag_id = %s AND state = 'success'
        GROUP BY task_id;
    """
    with pg_conn.cursor() as cur:
        cur.execute(query, (dag_id,))
        results = cur.fetchall()
    return {task_id: total_duration for task_id, total_duration in results}

def save_results(data, dag_id, filename_suffix):
    """Saves dictionary data to a pickle file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_path = os.path.join(OUTPUT_DIR, f"{dag_id}_{filename_suffix}.pkl")

    with open(file_path, "wb") as f:
        pickle.dump(data, f)  # Save using pickle

    print(f"Saved {filename_suffix} for DAG '{dag_id}' to {file_path}")

def connect_to_postgres(postgres_container):
    """Connect to Postgres DB inside Astronomer stack."""
    container_ip = postgres_container.attrs['NetworkSettings']['IPAddress']
    print(f"Postgres container IP: {container_ip}")
    config = POSTGRES_CONFIG.copy()
    #config['host'] = container_ip
    conn = psycopg2.connect(**config)
    return conn

def process_dag(dag_id, scheduler_container, pg_conn, N=20):
    """Full processing pipeline for a single DAG."""
    # Initialize log file
    initialize_log_file(scheduler_container, dag_id)

    # Execute DAG N times
    for _ in range(N):
        trigger_dag(dag_id)
        # Poll Postgres to check if DAG has finished
        polling_interval = 20
        elapsed_time = 0
        while True:
            time.sleep(polling_interval)
            elapsed_time += polling_interval
            with pg_conn.cursor() as cur:
                cur.execute("SELECT state FROM dag_run WHERE dag_id = %s ORDER BY execution_date DESC LIMIT 1", (dag_id,))
                state = cur.fetchone()
            if state and state[0] == "success":
                print(f"DAG '{dag_id}' succeeded!")
                break
            elif state and state[0] == "failed":
                print(f"DAG '{dag_id}' failed. Skipping to next DAG...")
                break
            elif not state:
                raise RuntimeError(f"Failed to fetch state for DAG '{dag_id}'.")
            else:
                print(f"Waiting for DAG '{dag_id}' to finish. Current state: {state[0]} (elapsed time: {elapsed_time} seconds)")
    

    # Fetch and parse timing log
    log_text = fetch_log_file(scheduler_container, dag_id)
    timing_data = parse_log_data(log_text)
    save_results(timing_data, dag_id, "timing_logs")

    # Fetch task durations from metadata DB
    task_durations = get_task_durations_from_db(dag_id, pg_conn)
    save_results(task_durations, dag_id, "task_durations")

# ----------- MAIN FUNCTION -------------

def main():
    start_airflow()

    scheduler_container = get_container_by_name("scheduler")
    postgres_container = get_container_by_name("postgres")

    # Connect to Postgres running inside Astronomer stack
    pg_conn = connect_to_postgres(postgres_container)

    try:
        for dag_id in DAGS_TO_RUN:
            process_dag(dag_id, scheduler_container, pg_conn)
    finally:
        pg_conn.close()
        print("Closed Postgres connection.")

if __name__ == "__main__":
    main()