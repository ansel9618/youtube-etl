from datetime import date
import datetime as dt
import great_expectations as gx
import pandas as pd

from airflow.decorators import task
from airflow.models import Variable

from api_script.video_stats import videos_count

#################################### STAGING QUALITY TESTS ####################################

# Getting staging_datasource using PostgresHook to connect to postgres database and then converting to dataframe using pandas


def staging_datasource(cur):
    # Selecting all rows from the Staging database
    cur.execute("""SELECT * FROM staging.yt_api;""")
    datasource = cur.fetchall()

    # Setting up dataframe
    columns = [
        "video_id",
        "video_title",
        "upload_date",
        "video_views",
        "likes_count",
        "comments_count",
    ]
    datasource = pd.DataFrame(datasource, columns=columns)
    datasource = gx.from_pandas(datasource)

    return datasource


# 'video_id' column data quality checks
@task(trigger_rule="none_failed_min_one_success")
def video_id_check(cur):
    datasource = staging_datasource(cur)

    expectations = [
        datasource.expect_column_values_to_not_be_null("video_id"),
        datasource.expect_column_values_to_be_unique("video_id"),
        datasource.expect_column_values_to_be_of_type("video_id", "object"),
        datasource.expect_column_value_lengths_to_equal("video_id", 11),
    ]
    for expectation in expectations:
        if expectation["success"] is False:
            raise gx.exceptions.GreatExpectationsError(expectation)


# 'video_title' column data quality checks
@task
def video_title_check(cur):
    datasource = staging_datasource(cur)

    expectations = [
        datasource.expect_column_values_to_not_be_null("video_title"),
        datasource.expect_column_values_to_be_of_type("video_title", "object"),
    ]
    for expectation in expectations:
        if expectation["success"] is False:
            raise gx.exceptions.GreatExpectationsError(expectation)


# 'upload_date' column data quality checks (for staging)
@task
def upload_date_staging_check(cur):
    datasource = staging_datasource(cur)

    expectations = [
        datasource.expect_column_values_to_not_be_null("upload_date"),
        datasource.expect_column_values_to_be_of_type("upload_date", "object"),
    ]
    for expectation in expectations:
        if expectation["success"] is False:
            raise gx.exceptions.GreatExpectationsError(expectation)


# 'video_views' column data quality checks
@task
def video_views_check(cur):
    datasource = staging_datasource(cur)

    expectations = [
        datasource.expect_column_values_to_not_be_null("video_views"),
        datasource.expect_column_values_to_be_of_type("video_views", "int64"),
        datasource.expect_column_pair_values_A_to_be_greater_than_B(
            "video_views", "likes_count", or_equal=True
        ),
        datasource.expect_column_pair_values_A_to_be_greater_than_B(
            "video_views", "comments_count", or_equal=True
        ),
    ]
    for expectation in expectations:
        if expectation["success"] is False:
            raise gx.exceptions.GreatExpectationsError(expectation)


# 'likes_count' column data quality checks
@task
def likes_count_check(cur):
    datasource = staging_datasource(cur)

    expectations = [
        datasource.expect_column_values_to_not_be_null("likes_count"),
        datasource.expect_column_values_to_be_of_type("likes_count", "int64"),
    ]
    for expectation in expectations:
        if expectation["success"] is False:
            raise gx.exceptions.GreatExpectationsError(expectation)


# 'comments_count' column data quality checks
@task
def comments_count_check(cur):
    datasource = staging_datasource(cur)

    expectations = [
        datasource.expect_column_values_to_not_be_null("comments_count"),
        datasource.expect_column_values_to_be_of_type("comments_count", "int64"),
    ]
    for expectation in expectations:
        if expectation["success"] is False:
            raise gx.exceptions.GreatExpectationsError(expectation)


# Check to verify that the number of rows in the database is equivalent to the number of videos
# Check is needed since the number of videos and the videos statistics come from 2 different YT API URLs
@task
def rows_check(cur):
    datasource = staging_datasource(cur)

    # Number of videos
    video_count = videos_count()[0]
    expectation = datasource.expect_table_row_count_to_equal(video_count)
    if expectation["success"] is False:
        raise gx.exceptions.GreatExpectationsError(expectation)


#################################### CORE QUALITY TESTS ####################################

# Getting core datasource using PostgresHook to connect to postgres database and then converting to dataframe using pandas


def core_datasource(cur):
    # Selecting all rows from the Staging database
    cur.execute("""SELECT * FROM core.yt_api;""")
    datasource = cur.fetchall()

    # Setting up dataframe
    columns = [
        "video_id",
        "video_title",
        "upload_date",
        "video_views",
        "likes_count",
        "comments_count",
    ]
    datasource = pd.DataFrame(datasource, columns=columns)
    datasource["upload_date"] = pd.to_datetime(datasource["upload_date"])
    datasource = gx.from_pandas(datasource)

    return datasource


# 'upload_date' column data quality checks (for core)
@task(trigger_rule="none_failed_min_one_success")
def upload_date_core_check(cur):
    datasource = core_datasource(cur)

    # Joined Date is the date when this specific channel was created
    JOINED_DATE = Variable.get("JOINED_DATE")
    JOINED_DATE = dt.datetime.strptime(JOINED_DATE, "%Y-%m-%d").date()

    # Getting today's date in string format
    today_date = date.today()

    expectations = [
        datasource.expect_column_values_to_be_between(
            "upload_date", JOINED_DATE, today_date
        )
    ]
    for expectation in expectations:
        if expectation["success"] is False:
            raise gx.exceptions.GreatExpectationsError(expectation)
