from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

from api_script.video_stats import video_stats
from database_scripts.first_pull import (
    staging_schema_exists,
    creating_staging_schema_w_table,
    insert_values_into_staging,
)
from database_scripts.successive_pulls import update_values, insert_newvalues
from data_quality_tests.data_quality import (
    video_id_check,
    video_title_check,
    upload_date_staging_check,
    video_views_check,
    likes_count_check,
    comments_count_check,
    rows_check,
    upload_date_core_check,
)
from database_scripts.core_schema import (
    core_schema_exists,
    create_core_schema,
    update_core_schema,
)

with DAG(
    dag_id="yt_etl",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    description="Showcasing use of the Youtube API by performing a simple ETL process",
    tags=["etl", "youtube", "api"],
    catchup=False,
):
    # DAG Global Variables

    # Setting up PostgresHook to connect to database
    # Note: 'database' argument used to be 'schema' in PostgresHook for previous Airflow versions
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_etl", database="ETL_YT_API")
    conn = hook.get_conn()
    cur = conn.cursor()

    # 1st step is to pull the data using the Youtube API and saving as json through video_stats function
    youtube_data_pull = PythonOperator(
        task_id="youtube_data_pull", python_callable=video_stats
    )

    # 2nd step is to determine whether staging schema already exists:

    # Branch 1:
    # If staging schema does not exist then create the staging schema [Using create_schemas() func]
    # Also, in the staging schema, create table & insert values i.e. first data pull from API
    # This part of the DAG should only be run ONCE i.e. when schema/ tables are being created

    @task.branch
    def branch_does_staging_schema_exist():
        schema_count = staging_schema_exists(cur)
        if schema_count == 0:
            return [
                "staging_schema_creation.creating_staging_schema_w_table",
                "staging_schema_creation.insert_values_into_staging",
            ]

        # 3rd Step is data quality testing of staging layer table

        # Branch 2:
        # Below part of if statement is executed for successive runs when schema/ tables already exist
        # Here we updated table with the new values and insert any new rows (corresponding to new videos)
        else:
            # Update database with updated values & Insert new rows based on video id
            return [
                "staging_schema_updating.update_values",
                "staging_schema_updating.insert_newvalues",
            ]

    # Grouping staging - first pull
    with TaskGroup(group_id="staging_schema_creation") as first_pull:
        tssc1 = creating_staging_schema_w_table(conn, cur)
        tssc2 = insert_values_into_staging(conn, cur)

        tssc1 >> tssc2

    # Grouping staging - successive pulls
    with TaskGroup(group_id="staging_schema_updating") as successive_pulls:
        tssu1 = update_values(conn, cur)
        tssu2 = insert_newvalues(conn, cur)

        tssu1 >> tssu2

    # 4th Step is data quality testing of staging layer table
    with TaskGroup(group_id="staging_quality_checks") as ge_staging:
        ge_sqc1 = video_id_check(cur)
        ge_sqc2 = video_title_check(cur)
        ge_sqc3 = upload_date_staging_check(cur)
        ge_sqc4 = video_views_check(cur)
        ge_sqc5 = likes_count_check(cur)
        ge_sqc6 = comments_count_check(cur)
        ge_sqc7 = rows_check(cur)

        ge_sqc1 >> ge_sqc2 >> ge_sqc3 >> ge_sqc4 >> ge_sqc5 >> ge_sqc6 >> ge_sqc7

    # 5th step is to determine whether core schema exists, this is based on staging schema

    @task.branch
    def branch_does_core_schema_exist():
        schema_count = core_schema_exists(cur)
        if schema_count == 0:
            return "create_core_schema"
        else:
            return "update_core_schema"

    # 6th step is data quality testing of core table
    with TaskGroup(group_id="core_quality_checks") as ge_core:
        ge_cqc1 = upload_date_core_check(cur)

    ############################ Creating DAG dependencies ############################

    (
        youtube_data_pull
        >> branch_does_staging_schema_exist()
        >> [first_pull, successive_pulls]
        >> ge_staging
        >> branch_does_core_schema_exist()
        >> [create_core_schema(conn, cur), update_core_schema(conn, cur)]
        >> ge_core
    )
