import airflow
from airflow import DAG
from utils.config import source_data
from utils.extract_to_staging import main as process

from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

###############################################
# Parameters
###############################################
postgres_driver_jar = "spark/resources/jars/postgresql-42.7.3.jar"
postgres_db = "jdbc:postgresql://postgres-staging:5432/staging_db"
postgres_user = "admin"
postgres_pwd = "admin"

source = {
  "dim_products": "products",
  "dim_suppliers": "suppliers",
  "dim_categories": "categories",
  "dim_employees": "employees",
  "dim_orders": "orders",
  "fact_order_details": "order_details"
}
###############################################
# DAG Definition
###############################################

with DAG(
    dag_id="spark_flow", 
    start_date=airflow.utils.dates.days_ago(1),
    default_args = {
      "owner": "Maulana Ahmad Maliki",
      "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
  ) as dag:

  start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
  )

  # [START task_extract_to_staging]
  with TaskGroup("extract_to_staging", tooltip="Tasks for Extract to Staging") as extract_to_staging:
    extract_tasks = [
      PythonOperator(
        task_id=f"table_{source_name}",
        python_callable=process,
        op_args=[source_url, source_name]
      ) for source_name, source_url in source_data.items()
    ]
    chain(*extract_tasks)
  # [END task_extract_to_staging]

  # [START task_spark]
  with TaskGroup("spark", tooltip="Tasks for Spark") as spark:
      
    # [START task_spark_extract]
    with TaskGroup("spark_extract", tooltip="Tasks for Spark extract") as spark_extract:
      extract_jobs = [
        SparkSubmitOperator(
          task_id=f"extract_{source_name}_job",
          conn_id="spark-conn",
          application="spark/jobs/extract.py",
          total_executor_cores=4,
          executor_cores=2,
          executor_memory='2g',
          driver_memory='1g',
          jars=postgres_driver_jar,
          driver_class_path=postgres_driver_jar,
          application_args=[
            postgres_db,
            postgres_user,
            postgres_pwd,
            source_table,
            source_name
          ]
        ) for source_name, source_table in source.items()
      ]
      chain(*extract_jobs)
    # [END task_spark_extract]

    # [START task_spark_transform]
    with TaskGroup("spark_transform", tooltip="Tasks for Spark transform") as spark_transform:
      supplier_monthly_revenue = SparkSubmitOperator(
        task_id="supplier_monthly_revenue",
        conn_id="spark-conn",
        application="spark/jobs/supplier_monthly_revenue.py",
        total_executor_cores=4,
        executor_cores=2,
        executor_memory='2g',
        driver_memory='2g',
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
        application_args=[
          postgres_db,
          postgres_user,
          postgres_pwd
        ]
      )
      top_selling_category = SparkSubmitOperator(
        task_id="top_selling_category",
        conn_id="spark-conn",
        application="spark/jobs/top_selling_category.py",
        total_executor_cores=4,
        executor_cores=2,
        executor_memory='2g',
        driver_memory='2g',
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
        application_args=[
          postgres_db,
          postgres_user,
          postgres_pwd
        ]
      )
      top_employee_by_revenue = SparkSubmitOperator(
        task_id="top_employee_by_revenue",
        conn_id="spark-conn",
        application="spark/jobs/top_employee_by_revenue.py",
        total_executor_cores=4,
        executor_cores=2,
        executor_memory='2g',
        driver_memory='2g',
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
        application_args=[
          postgres_db,
          postgres_user,
          postgres_pwd
        ]
      )
      supplier_monthly_revenue >> top_selling_category >> top_employee_by_revenue
    # [END task_spark_transform]

    spark_extract >> spark_transform 
  # [END task_spark]

  with TaskGroup("load", tooltip="Tasks for Load") as load:
    # explicit append for datamart table
    source["dm_supplier_monthly_revenue"] = "supplier_monthly_revenue"
    source["dm_top_selling_category"] = "top_selling_category"
    source["dm_top_employee_by_revenue"] = "top_employee_by_revenue"

    with TaskGroup("load_gcs", tooltip="Tasks for Load") as load_gcs:
      local_to_gcs = [
          LocalFilesystemToGCSOperator(
            task_id = f"local_to_gcs_{source_name}",
            src = f"output/{source_name}.csv",
            dst = f"dwh/{source_name}.csv",
            bucket = "project-mart-bucket"
          ) for source_name in source.keys()
        ]
      
      chain(*extract_jobs)
    with TaskGroup("load_bq", tooltip="Tasks for Load") as load_bq:
      gcp_to_bigquery = [
        GCSToBigQueryOperator(
          task_id = f"load_to_bigquery_{source_name}",
          bucket = "project-mart-bucket",
          source_objects=[f"dwh/{source_name}.csv"],
          field_delimiter =",",
          destination_project_dataset_table = f"project-mart-427214.datamart.{source_name}",
          write_disposition = "WRITE_TRUNCATE"
        ) for source_name in source.keys()
      ]
    load_gcs >> load_bq

  end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
  )

  # Setting dependencies
  start >> extract_to_staging >> spark >> load >> end