import airflow
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


with DAG(
    dag_id="gcp_task", 
    start_date=airflow.utils.dates.days_ago(1),
    default_args={
      "owner": "Maulana Ahmad Maliki",
      "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Jobs started"),
    )

    local = LocalFilesystemToGCSOperator(
        task_id = 'local',
        src = f"output/dim_categories.csv",
        dst = f"dwh/dim_categories.csv",
        bucket = "project-mart-bucket"
    )

    load_zone_map_to_bigquery = GCSToBigQueryOperator(
        task_id = 'load_zone_map_to_bigquery',
        bucket = "project-mart-bucket",
        source_objects=['dwh/dim_categories.csv'],
        field_delimiter =',',
        destination_project_dataset_table = "project-mart-427214.datamart.dim_categories",
        write_disposition = 'WRITE_TRUNCATE'
    )

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Jobs completed successfully"),
    )

    # Setting dependencies
    start >> local >> load_zone_map_to_bigquery >> end
