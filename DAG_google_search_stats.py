from datetime import datetime
import pandas as pd
import json

from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


dag = DAG(
    dag_id = "google_search",
    start_date = datetime(2021, 2, 18),
    schedule_interval = None,           #change as required
)

fetch_searches = BashOperator(
    task_id = "fetch_searches",
    bash_command = "curl -o /home/ddyakov/airflow/google_searches/searches.json --request GET\
         --url 'https://google-search3.p.rapidapi.com/api/v1/search/q=bitcoin+bubble&num=10'\
         --header 'x-rapidapi-host: google-search3.p.rapidapi.com'\
         --header 'x-rapidapi-key: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'",    #see if credentials can be kept externally
    dag=dag,
)

def calculate_title_length(input_path, output_path):
    with open(input_path) as json_data:
        data = json.load(json_data)
        searches = pd.DataFrame(data["results"])
        searches["stats"] = searches["title"].str.len()
        stats = searches.groupby(["stats"]).size().reset_index()
        stats.to_csv(output_path, index = False)

calculate_stats = PythonOperator(
    task_id = "calculate_stats",
    python_callable = calculate_title_length,
    op_kwargs={
        "input_path": "/google_searches/searches.json",
        "output_path": "/google_searches/stats.csv"
    },
    dag = dag
)

fetch_searches >> calculate_stats
