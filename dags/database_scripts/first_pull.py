from airflow.decorators import task
from api_script.video_stats import load_data


# Function that checks whether staging schema exists in the database
# If empty, database is created - Only needed to run once
def staging_schema_exists(cur):
    cur.execute(
        "SELECT COUNT(schema_name) FROM information_schema.schemata WHERE schema_name = 'staging';"
    )
    schema_count = int(cur.fetchone()[0])

    return schema_count


# Function that creates staging schema & table of Data Warehouse (only needed for 1st pull)
@task
def creating_staging_schema_w_table(con, cur):
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS staging.yt_api (
            Video_ID VARCHAR(11) PRIMARY KEY NOT NULL,
            Video_Title TEXT NOT NULL,
            Upload_Date VARCHAR(20) NOT NULL,
            Video_Views INT NOT NULL,
            Likes_Count INT NOT NULL,
            Comments_Count INT NOT NULL    
        ); 
        """
    )

    # Commit changes
    con.commit()


# If database is empty then function insert_values() is used
# Only needed for 1st time insert to populate database
@task
def insert_values_into_staging(con, cur):
    YT_data = load_data()

    for row in YT_data:
        # The below if conditition originates from a great expectations data quality check
        # Also int() is needed because they are read as string values
        if int(row["Video_Views"]) >= int(row["Likes_Count"]):
            cur.execute(
                """INSERT INTO staging.yt_api(Video_ID, Video_Title, Upload_Date, Video_Views, Likes_Count, Comments_Count)
                        VALUES (%(Video_ID)s,%(Video_Title)s,%(Upload_Date)s,%(Video_Views)s,%(Likes_Count)s,
                        %(Comments_Count)s);""",
                row,
            )
    # Commit changes
    con.commit()
