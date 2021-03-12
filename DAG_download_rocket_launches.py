import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id = "download_rocket_launches",
    start_date=airflow.utils.dates.days_ago(30),
    schedule_interval=None                          #change based on project specifics
)

download_launches = BashOperator(
    task_id = "download_launches",
    bash_command="curl -o  /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag
)

def get_pictures():
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    with open("/tmp/launches.json") as file:
        launches = json.load(file)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be invalid.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}")

manage_pictures = PythonOperator(
    task_id="manage_pictures",
    python_callable=get_pictures,
    dag=dag
)

notify = BashOperator(
    task_id="notify",
    bash_command="echo 'There are now $(ls /tmp/images | wc -l) images.'",
    dag=dag
)

download_launches >> manage_pictures >> notify 


        

