import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_flow_test", 
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

    # [START task_transfrom]
    python_job = SparkSubmitOperator(
      task_id="python_job",
      conn_id="spark-conn",
      application="spark/jobs/hello-word-spark.py"
    )
    # [END task_transfrom]


    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Jobs completed successfully"),
    )

    # Setting dependencies
    start >> python_job >> end
