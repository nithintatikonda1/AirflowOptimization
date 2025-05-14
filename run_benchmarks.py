import subprocess
import time
import os
import psycopg2


# ----------- CONFIGURATIONS --------------
DAGS_TO_RUN = ['aws_change', 'bedrock_blog_generator', 'patent_crawler', "simple_redshift_1", "gx_pandas", "find_the_iss", 'FinSum_OpenAI',
               "push_pull", "register_mlflow", 's3_upload', 's3_upload_copy', 'manatee_sentiment', 'stock', 'telephone_game_1', 'texas_hold_em',
               'text_processing','ml_pipeline', 'batch_data_dag'] 
DAGS_TO_RUN.remove('FinSum_OpenAI')
DAGS_TO_RUN.remove('bedrock_blog_generator')
DAGS_TO_RUN.remove('s3_upload_copy')
DAGS_TO_RUN.remove('aws_change')
DAGS_TO_RUN.remove('patent_crawler')
DAGS_TO_RUN = ['push_pull']
# Postgres connection details
POSTGRES_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432,
}

OUTPUT_FILE = "./results.txt"

def unpause_dag(dag_id):
    print(f"Unpausing DAG: {dag_id}")
    subprocess.run(["astro", "dev", "run", "dags", "unpause", dag_id], check=True, stdout = subprocess.DEVNULL)

def pause_dag(dag_id):
    print(f"Pausing DAG: {dag_id}")
    subprocess.run(["astro", "dev", "run", "dags", "pause", dag_id], check=True, stdout = subprocess.DEVNULL)

def trigger_dag(dag_id):
    """Triggers a single DAG run."""
    print(f"Triggering DAG: {dag_id}")
    subprocess.run(["astro", "dev", "run", "dags", "trigger", dag_id], check=True, stdout = subprocess.DEVNULL)


def get_dag_duration_from_db(dag_id, pg_conn):
    """Fetches total duration of DAG from Airflow metadata database."""
    print(f"Querying duration for DAG '{dag_id}'")
    query = """SELECT
        dag_id,
        run_id,
        start_date,
        end_date,
        EXTRACT(EPOCH FROM (end_date - start_date)) AS execution_time_seconds
    FROM
        dag_run
    WHERE
        dag_id = %s AND state = 'success'
    ORDER BY
        start_date DESC LIMIT 30
     """

    with pg_conn.cursor() as cur:
        cur.execute(query, (dag_id,))
        result = cur.fetchall()
    if len(result) == 0:
        return -1
    ans = sum(row[4] for row in result) / len(result)
    return ans


def connect_to_postgres():
    """Connect to Postgres DB inside Astronomer stack."""
    config = POSTGRES_CONFIG.copy()
    #config['host'] = container_ip
    conn = psycopg2.connect(**config)
    return conn

def process_dag(dag_id, pg_conn, N):
    """Full processing pipeline for a single DAG."""
    # Execute DAG N times
    unpause_dag(dag_id)
    failed = 0
    for _ in range(N):
        trigger_dag(dag_id)
        # Poll Postgres to check if DAG has finished
        polling_interval = 10
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
                failed += 1
                break
            elif not state:
                raise RuntimeError(f"Failed to fetch state for DAG '{dag_id}'.")
            else:
                print(f"Waiting for DAG '{dag_id}' to finish. Current state: {state[0]} (elapsed time: {elapsed_time} seconds)")


    # Fetch task durations from metadata DB
    dag_duration = get_dag_duration_from_db(dag_id, pg_conn)

    #Write dag duration to file
    with open(OUTPUT_FILE, "a") as f:
        f.write(f"{dag_id} {dag_duration}\n")

    # Pause DAG
    pause_dag(dag_id)

    return dag_duration, failed

# ----------- MAIN FUNCTION -------------

def main():
    N = 30
    old_N = N

    # Connect to Postgres running inside Astronomer stack
    pg_conn = connect_to_postgres()

    durations = {}
    failed = {}
    try:
        for dag_id in DAGS_TO_RUN:
            durations[dag_id], failed[dag_id] = process_dag(dag_id, pg_conn, N)
            #durations[dag_id + "_f_optimized"], failed[dag_id + "_f_optimized"] = process_dag(dag_id + "_f_optimized", pg_conn, N)
            durations[dag_id + "_fp_optimized"], failed[dag_id + "_fp_optimized"] = process_dag(dag_id + "_fp_optimized", pg_conn, N)
    finally:
        pg_conn.close()
        print("Closed Postgres connection.")

    print(durations)
    print(failed)

if __name__ == "__main__":
    #{'patent_crawler': 3.308821333333333, 'patent_crawler_optimized': 3.159565666666667, 'simple_redshift_1': 3.638294333333333, 'simple_redshift_1_optimized': 1.7491}
    main()