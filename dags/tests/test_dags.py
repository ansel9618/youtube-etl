from datetime import datetime, timezone

# https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html


# Unit test for loading a DAG:
# We check for:
# 1. No import errors,
# 2. dag exists,
# 3. Number of tasks in the dag
def test_dag_no_import_errors(dagbag):
    assert len(dagbag.import_errors) == 0

    
def test_dag_exists(dagbag):
    dag = dagbag.get_dag(dag_id="yt_etl")
    assert dag is not None


def test_sag_task_count(dagbag):
    dag = dagbag.get_dag(dag_id="yt_etl")
    assert len(dag.tasks) == 17


# Making sure DAG:
# 1. Starts at the correct date and that DAG
# 2. Runs once a day at midnight
def test_dag_starts_and_is_scheduled_correctly(dagbag):
    dag = dagbag.get_dag(dag_id="yt_etl")

    assert dag.start_date == datetime(2023, 1, 1, tzinfo=timezone.utc)
    assert dag.schedule_interval == "@daily"


# Asserting that tasks are ordered correctly
def test_dag_has_correct_task_order(dagbag):
    dag = dagbag.get_dag(dag_id="yt_etl")

    extract_data_pull = dag.get_task(task_id="youtube_data_pull")
    does_staging_schema_exist = dag.get_task(task_id="branch_does_staging_schema_exist")
    first_data_pull_1 = dag.get_task(
        task_id="staging_schema_creation.creating_staging_schema_w_table"
    )
    first_data_pull_2 = dag.get_task(
        task_id="staging_schema_creation.insert_values_into_staging"
    )
    successive_data_pull_1 = dag.get_task(
        task_id="staging_schema_updating.update_values"
    )
    successive_data_pull_2 = dag.get_task(
        task_id="staging_schema_updating.insert_newvalues"
    )
    staging_quality_check_1 = dag.get_task(
        task_id="staging_quality_checks.video_id_check"
    )
    staging_quality_check_2 = dag.get_task(
        task_id="staging_quality_checks.video_title_check"
    )
    staging_quality_check_3 = dag.get_task(
        task_id="staging_quality_checks.upload_date_staging_check"
    )
    staging_quality_check_4 = dag.get_task(
        task_id="staging_quality_checks.video_views_check"
    )
    staging_quality_check_5 = dag.get_task(
        task_id="staging_quality_checks.likes_count_check"
    )
    staging_quality_check_6 = dag.get_task(
        task_id="staging_quality_checks.comments_count_check"
    )
    staging_quality_check_7 = dag.get_task(task_id="staging_quality_checks.rows_check")
    does_core_schema_exist = dag.get_task(task_id="branch_does_core_schema_exist")
    core_quality_checks_1 = dag.get_task(
        task_id="core_quality_checks.upload_date_core_check"
    )

    assert extract_data_pull.downstream_list == [does_staging_schema_exist]
    assert does_staging_schema_exist.downstream_task_ids == {
        "staging_schema_updating.update_values",
        "staging_schema_creation.creating_staging_schema_w_table",
    }
    assert first_data_pull_1.downstream_list == [first_data_pull_2]
    assert successive_data_pull_1.downstream_list == [successive_data_pull_2]
    assert staging_quality_check_1.upstream_task_ids == {
        "staging_schema_updating.insert_newvalues",
        "staging_schema_creation.insert_values_into_staging",
    }
    assert staging_quality_check_1.downstream_list == [staging_quality_check_2]
    assert staging_quality_check_2.downstream_list == [staging_quality_check_3]
    assert staging_quality_check_3.downstream_list == [staging_quality_check_4]
    assert staging_quality_check_4.downstream_list == [staging_quality_check_5]
    assert staging_quality_check_5.downstream_list == [staging_quality_check_6]
    assert staging_quality_check_6.downstream_list == [staging_quality_check_7]
    assert staging_quality_check_7.downstream_list == [does_core_schema_exist]
    assert does_core_schema_exist.downstream_task_ids == {
        "create_core_schema",
        "update_core_schema",
    }
    assert core_quality_checks_1.upstream_task_ids == {
        "create_core_schema",
        "update_core_schema",
    }


# Asserting that tasks are triggering on the correct rules:
# By default, the trigger rule is all success so tasks with these trigger
# rules will not be tested
def test_trigger_rules(
    dagbag,
):
    dag = dagbag.get_dag(dag_id="yt_etl")

    first_task_staging_quality_checks = dag.get_task(
        task_id="staging_quality_checks.video_id_check"
    )
    core_quality_check_upload_date = dag.get_task(
        task_id="core_quality_checks.upload_date_core_check"
    )

    assert (
        first_task_staging_quality_checks.trigger_rule == "none_failed_min_one_success"
    )
    assert core_quality_check_upload_date.trigger_rule == "none_failed_min_one_success"
