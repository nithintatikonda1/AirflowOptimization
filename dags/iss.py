"""
## Find the International Space Station

This DAG waits for a specific commit message to appear in a GitHub repository, 
and then will pull the current location of the International Space Station from an API
and print it to the logs.

This DAG needs a GitHub connection with the name `my_github_conn` and 
an HTTP connection with the name `open_notify_api_conn`
and the host `https://api.open-notify.org/` to work.

Additionally you will need to set an Airflow variable with 
the name `open_notify_api_endpoint` and the value `iss-now.json`.
"""

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.http.operators.http import HttpOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
from pendulum import datetime
from typing import Any
import logging
from airflowfusion.backend_registry import read, write
from airflowfusion.fuse import create_optimized_dag

task_logger = logging.getLogger("airflow.task")

YOUR_GITHUB_REPO_NAME = Variable.get(
    "my_github_repo", "apache/airflow"
)  # Replace with your repository name
YOUR_COMMIT_MESSAGE = "Where is the ISS right now?"  # Replace with your commit message


def commit_message_checker(repo: Any, trigger_message: str) -> bool | None:
    """Check the last 10 commits to a repository for a specific message.
    Args:
        repo (Any): The GitHub repository object.
        trigger_message (str): The commit message to look for.
    """

    task_logger.info(
        f"Checking for commit message: {trigger_message} in 10 latest commits to the repository {repo}."
    )

    result = None
    try:
        if repo is not None and trigger_message is not None:
            commits = repo.get_commits().get_page(0)[:10]
            for commit in commits:
                if trigger_message in commit.commit.message:
                    result = True
                    break
            else:
                result = False

    except Exception as e:
        raise AirflowException(f"GitHub operator error: {e}")
    return result


@dag(
    start_date=datetime(2023, 6, 1),
    catchup=False,
    tags=["Connections"],
)
def find_the_iss():


    get_iss_coordinates = HttpOperator(
        task_id="get_iss_coordinates",
        http_conn_id="open_notify_api_conn",
        endpoint="iss-now.json",
        method="GET",
        log_response=True,
    )
    

    @task
    def log_iss_location(location: str) -> dict:
        """
        This task prints the current location of the International Space Station to the logs.
        Args:
            location (str): The JSON response from the API call to the Open Notify API.
        Returns:
            dict: The JSON response from the API call to the Reverse Geocode API.
        """
        import requests
        import json

        location_dict = json.loads(location)

        lat = location_dict["iss_position"]["latitude"]
        lon = location_dict["iss_position"]["longitude"]

        r = requests.get(
            f"https://api.bigdatacloud.net/data/reverse-geocode-client?{lat}?{lon}"
        ).json()

        country = r["countryName"]
        city = r["locality"]

        task_logger.info(
            f"The International Space Station is currently over {city} in {country}."
        )

        return r

    log_iss_location_obj = log_iss_location(get_iss_coordinates.output)

    chain(get_iss_coordinates, log_iss_location_obj)


dag = find_the_iss()
fused_dag = create_optimized_dag(dag, timing=True)