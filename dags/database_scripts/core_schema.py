from airflow.decorators import task

# Function that checks whether core schema exists in the database
# If empty, database is created - Only needed to run once


def core_schema_exists(cur):
    cur.execute(
        "SELECT COUNT(schema_name) FROM information_schema.schemata WHERE schema_name = 'core';"
    )
    schema_count = int(cur.fetchone()[0])

    return schema_count


core_sql = """
                CREATE TABLE IF NOT EXISTS core.yt_api AS
                WITH core_cte AS (
                SELECT
                    Video_ID, 
                    Video_Title, 
                    Upload_Date, 
                    Video_Views, 
                    Likes_Count, 
                    Comments_Count 
                FROM 
                    staging.yt_api
                )
                SELECT
                    Video_ID, 
                    Video_Title, 
                    (SPLIT_PART(Upload_Date,'T',1))::DATE AS Upload_Date, 
                    Video_Views, 
                    Likes_Count, 
                    Comments_Count
                FROM 
                    core_cte;
            """


# Function that creates core schema and associated table of Data Warehouse
@task
def create_core_schema(con, cur):
    cur.execute("CREATE SCHEMA IF NOT EXISTS core;")
    cur.execute(core_sql)

    # Commit changes
    con.commit()


# Function copies staging layer table, perfroms minor transformations & creates the core layer table
@task
def update_core_schema(con, cur):
    cur.execute("DROP TABLE core.yt_api;")
    cur.execute(core_sql)

    # Commit changes
    con.commit()
