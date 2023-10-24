from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'tarifi',
    'start_date': datetime(2023, 10, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create an instance of the DAG
dag = DAG(
    'task_to_run',
    default_args=default_args,
    description='Data pipeline for collecting and analyzing data from Mastodon',
    schedule_interval=timedelta(minutes=1),
    catchup=False,
)

# Run Task to get data
run_get_data = BashOperator(
    task_id='run_get_data_task',
    bash_command="python3 /home/TarifiHadoopAdmin/repositories/Mastadon_HadoopAirflow/extract/script.py",
    dag=dag,
)

# Task to run the Mapreduce script
run_mapreducer = BashOperator(
    task_id='run_mapreducer_task',
    bash_command="python3 /home/TarifiHadoopAdmin/repositories/Mastadon_HadoopAirflow/mapreduce/mr.py /home/TarifiHadoopAdmin/repositories/Mastadon_HadoopAirflow/posts122152.json > ./output.txt",
    dag=dag,
)

# TAsk to run the loading script
run_inset_hbase = BashOperator(
    task_id='insert_into_hbase',
    bash_command='python3 /home/TarifiHadoopAdmin/repositories/Mastadon_HadoopAirflow/load/hbase.py',
    dag=dag
)

# Set task dependencies
run_get_data >> run_mapreducer >> run_inset_hbase

if __name__ == "__main__":
    dag.cli()