import json
import yaml
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


AIRFLOW_ROOT = "/opt/airflow"
DEFAULT_AIRFLOW_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "cc_link_extractor",
    default_args=DEFAULT_AIRFLOW_ARGS,
    description="Extract and analyze external links from CommonCrawl",
    start_date=datetime.now(),
    catchup=False,
    # Default Spark parameters
    params={
        "app_name": "CC Link Extractor",
        "master": "local[*]",
        "executor_memory": "4g",
        "driver_memory": "2g",
        "jars": "postgresql-42.5.1.jar"
    }
)


def create_pipeline_config(**context: Dict[str, Any]) -> str:
    """Create the initial configuration for the pipeline (Step 0)

    Draws the pipeline parameter from `config-default/cc_link_extraction.json`

    Args:
        context: General DAG-level parameters (Spark configuration)

    Returns:
        Path to full pipeline config in YAML format
    """
    with open(AIRFLOW_ROOT + "config-default/cc_link_extraction.json", "r") as f_in:
        config = json.load(f_in)

    # Write the final config to a local YAML file
    ts = context['ts_nodash']
    config_path = AIRFLOW_ROOT + f"/configs/config_{ts}.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    return config_path


# Create config file task
create_pipeline_config_task = PythonOperator(
    task_id='create_pipeline_config',
    python_callable=create_pipeline_config,
    provide_context=True,
    dag=dag,
)

# Package the application
package_app_task = BashOperator(
    task_id='package_app',
    bash_command='docker-compose exec -T spark-app bash -c "cd /app && pip wheel -w dist/ ."',
    dag=dag,
)

# Step 1: Extract external links from CommonCrawl
extract_links_task = BashOperator(
    task_id='extract_links',
    bash_command='''
    docker-compose exec -T spark-app bash -c "cd /app && \
    spark-submit --master {{ params.master }} \
                 --driver-memory {{ params.driver_memory }} \
                 --executor-memory {{ params.executor_memory }} \
                 --jars {{ params.jars }} \
                 --name '{{ params.app_name }}' \
                 --py-files /app/dist/*.whl \
                 -m cc_link_extractor -s extract -c /app/configs/$(basename {{ ti.xcom_pull(task_ids='create_config') }})"
    ''',
    dag=dag,
)

# Step 2: Analyze external links
analyze_links_task = BashOperator(
    task_id='analyze_links',
    bash_command='''
    docker-compose exec -T spark-app bash -c "cd /app && \
    spark-submit --master {{ params.master }} \
                 --driver-memory {{ params.driver_memory }} \
                 --executor-memory {{ params.executor_memory }} \
                 --jars {{ params.jars }} \
                 --name '{{ params.app_name }}' \
                 --py-files /app/dist/*.whl \
                 -m cc_link_extractor -s analyze -c /app/configs/$(basename {{ ti.xcom_pull(task_ids='create_config') }})"
    ''',
    dag=dag,
)

# Set task dependencies
create_pipeline_config_task >> package_app_task >> extract_links_task >> analyze_links_task
