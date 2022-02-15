import configparser
from datetime import datetime, timedelta
import os
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from operators.data_quality import DataQualityOperator

from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

dag_path = "./dags/"
config = configparser.ConfigParser()
config.read(dag_path+'dl.cfg')
BUCKET_NAME = config['S3']['BUCKET_NAME'] 

local_script = dag_path+"scripts/spark/data_preparation.py"
s3_script = "scripts/data_preparation.py"
s3_clean = "data/clean-data/"
SPARK_STEPS = []


def _local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    """
    This is a helper function that transfers a file to an S3 bucket
    """
    s3 = S3Hook(aws_conn_id='aws_credentials')
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)


default_args = {
    'owner': 'David',
    'start_date': datetime(2018, 11, 1),
    #'start_date': datetime.now(), # for quick testing
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': True,
    'depends_on_past': True,
}

dag = DAG(
        'dag_immigration',
        start_date=datetime.now())

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)


# Transfer the data preparation script to S3
script_to_s3 = PythonOperator(
    dag=dag,
    task_id="script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": local_script, "key": s3_script,},
)

JOB_FLOW_OVERRIDES = {
    "Name": "Prepare Immigration Data",
    "ReleaseLabel": "emr-6.4.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # The EMR cluster will have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, ensure it uses py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", 
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this will provide permission to to programmaticaly terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_credentials",
    emr_conn_id="emr",
    region_name="us-west-2",
    dag=dag,
)

# These are the steps that are added to the EMR cluster. The params values are supplied to the operator
SPARK_STEPS = [ 
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/data/raw-data",
                "--dest=/raw-data",
            ],
        },
    },
    {
        "Name": "Prepare Data by running the data preparation script",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/clean-data",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_clean }}",
            ],
        },
    },
]


# add the steps (SPARK_STEPS to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials",
    steps=SPARK_STEPS,
    params={ #these params are passed to the paramterized values in SPARK_STEPS json
        "BUCKET_NAME": BUCKET_NAME,
        "s3_script": s3_script,
        "s3_clean": s3_clean,
    },
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
# This sensor waits for the last step to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_credentials",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials",
    dag=dag,
)

# Transfer airlines data to the dim_airlines table 
airlines_to_redshift = S3ToRedshiftOperator(
        s3_bucket=BUCKET_NAME,
        s3_key="data/clean-data/dim_airlines.parquet",
        schema="PUBLIC",
        table="dim_airlines",
        copy_options=['parquet'],        
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        method="REPLACE",
        task_id="airlines_to_redshift",
)

# Transfer countries data to the dim_countries table
countries_to_redshift = S3ToRedshiftOperator(
        s3_bucket=BUCKET_NAME,
        s3_key="data/clean-data/dim_countries.parquet",
        schema="PUBLIC",
        table="dim_countries",
        copy_options=['parquet'],        
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        method="REPLACE",
        task_id="countries_to_redshift",
)

# Transfer date data to the dim_date table 
date_to_redshift = S3ToRedshiftOperator(
        s3_bucket=BUCKET_NAME,
        s3_key="data/clean-data/dim_date.parquet",
        schema="PUBLIC",
        table="dim_date",
        copy_options=['parquet'],        
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        method="REPLACE",
        task_id="date_to_redshift",
)

# Transfer port of entry data to the dim_port_of_entry table 
port_of_entry_to_redshift = S3ToRedshiftOperator(
        s3_bucket=BUCKET_NAME,
        s3_key="data/clean-data/dim_port_of_entry.parquet",
        schema="PUBLIC",
        table="dim_port_of_entry",
        copy_options=['parquet'],        
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        method="REPLACE",
        task_id="port_of_entry_to_redshift",
)

# Transfer states data to the dim_states table 
states_to_redshift = S3ToRedshiftOperator(
        s3_bucket=BUCKET_NAME,
        s3_key="data/clean-data/dim_states.parquet",
        schema="PUBLIC",
        table="dim_states",
        copy_options=['parquet'],        
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        method="REPLACE",
        task_id="states_to_redshift",
)

# Transfer travel modes data to the dim_travel_modes table 
travel_modes_to_redshift = S3ToRedshiftOperator(
        s3_bucket=BUCKET_NAME,
        s3_key="data/clean-data/dim_travel_modes.parquet",
        schema="PUBLIC",
        table="dim_travel_modes",
        copy_options=['parquet'],        
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        method="REPLACE",
        task_id="travel_modes_to_redshift",
)

# Transfer visa categorie data to the dim_visa_categories table 
visa_categories_to_redshift = S3ToRedshiftOperator(
        s3_bucket=BUCKET_NAME,
        s3_key="data/clean-data/dim_visa_categories.parquet",
        schema="PUBLIC",
        table="dim_visa_categories",
        copy_options=['parquet'],        
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        method="REPLACE",
        task_id="visa_categories_to_redshift",
)

# Transfer immigration data to the fact_immigration table 
immigration_to_redshift = S3ToRedshiftOperator(
        s3_bucket=BUCKET_NAME,
        s3_key="data/clean-data/fact_immigration.parquet",
        schema="PUBLIC",
        table="fact_immigration",
        copy_options=['parquet'],        
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        method="REPLACE",
        task_id="immigration_to_redshift",
)

# Transfer temperature data to the fact_temperature table 
temperature_to_redshift = S3ToRedshiftOperator(
        s3_bucket=BUCKET_NAME,
        s3_key="data/clean-data/fact_temperature.parquet",
        schema="PUBLIC",
        table="fact_temperature",
        copy_options=['parquet'],        
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        method="REPLACE",
        task_id="temperature_to_redshift",
)

# Transfer US population data to the fact_us_population table 
us_population_to_redshift = S3ToRedshiftOperator(
        s3_bucket=BUCKET_NAME,
        s3_key="data/clean-data/fact_us_population.parquet",
        schema="PUBLIC",
        table="fact_us_population",
        copy_options=['parquet'],        
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        method="REPLACE",
        task_id="us_population_to_redshift",
)

# Transfer US race data to the fact_us_race table 
us_race_to_redshift = S3ToRedshiftOperator(
        s3_bucket=BUCKET_NAME,
        s3_key="data/clean-data/fact_us_race.parquet",
        schema="PUBLIC",
        table="fact_us_race",
        copy_options=['parquet'],        
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        method="REPLACE",
        task_id="us_race_to_redshift",
)

# run data quality checks
run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    tables=[
            {'name': 'dim_airlines', 'column_name':'airline_name'}, 
            {'name': 'dim_countries', 'column_name':'country_name'}, 
            {'name': 'dim_date', 'column_name':'date'},
            {'name': 'dim_port_of_entry', 'column_name':'port_of_entry_name'},
            {'name': 'dim_states', 'column_name':'state_name'},
            {'name': 'dim_travel_modes', 'column_name':'travel_mode_name'},
            {'name': 'dim_visa_categories', 'column_name':'visa_category_name'},
            {'name': 'fact_immigration', 'column_name':'admission_number'},
            {'name': 'fact_us_population', 'column_name':'city'},
            {'name': 'fact_us_race', 'column_name':'city'},
        ],
    redshift_conn_id="redshift",
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)


start_data_pipeline >> script_to_s3 >> create_emr_cluster
create_emr_cluster >> step_adder >> step_checker
step_checker >> terminate_emr_cluster
step_checker >>  [
    airlines_to_redshift, 
    countries_to_redshift, 
    date_to_redshift, 
    port_of_entry_to_redshift,
    states_to_redshift,
    travel_modes_to_redshift,
    visa_categories_to_redshift,
    immigration_to_redshift,
    temperature_to_redshift,
    us_population_to_redshift,
    us_race_to_redshift
]

[
    airlines_to_redshift, 
    countries_to_redshift, 
    date_to_redshift, 
    port_of_entry_to_redshift,
    states_to_redshift,
    travel_modes_to_redshift,
    visa_categories_to_redshift,
    immigration_to_redshift,
    temperature_to_redshift,
    us_population_to_redshift,
    us_race_to_redshift
] >> run_quality_checks

run_quality_checks >> end_data_pipeline