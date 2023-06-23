import itertools

from airflow.decorators import task
from api_script.video_stats import load_data


# Update those columns that can be edited i.e. Video_Title, Video_Views, Likes_Count, Comments_Count
@task
def update_values(con, cur):
    cur.execute("""SELECT Video_ID FROM staging.yt_api;""")
    ids = cur.fetchall()
    # Need to flatten list since fetchall outputs a list with elements as tuples
    ids = list(itertools.chain.from_iterable(ids))

    YT_data = load_data()

    # Go through each video_id and update the 4 fields which can change
    for row in YT_data:
        if row["Video_ID"] in ids:
            cur.execute(
                """
            UPDATE 
                staging.yt_api
            SET 
                Video_Title = %(Video_Title)s,
                Video_Views = %(Video_Views)s, 
                Likes_Count = %(Likes_Count)s, 
                Comments_Count = %(Comments_Count)s
            WHERE 
                Video_ID = %(Video_ID)s AND Upload_Date = %(Upload_Date)s;""",
                row,
            )

    # Commit changes
    con.commit()


# Insert new rows (videos) if the video ID is not in the current database
@task
def insert_newvalues(con, cur):
    # Fetching the last inputted video ID (by date) in the database
    cur.execute("""SELECT Video_ID FROM staging.yt_api ORDER BY Upload_Date DESC;""")
    last_inputted_ID = cur.fetchone()[0]

    YT_data = load_data()

    # Keep on inserting rows until 'Video_ID' is already in the database
    for row in YT_data:
        # The below if conditition originates from a great expectations data quality check
        # Also int() is needed because they are read as string values
        if int(row["Video_Views"]) >= int(row["Likes_Count"]):
            if row["Video_ID"] == last_inputted_ID:
                break
            else:
                cur.execute(
                    """
                INSERT INTO staging.yt_api(Video_ID, Video_Title, Upload_Date, Video_Views, Likes_Count, Comments_Count)
                VALUES (%(Video_ID)s,%(Video_Title)s,%(Upload_Date)s,%(Video_Views)s,%(Likes_Count)s,
                        %(Comments_Count)s);""",
                    row,
                )

    # Commit changes
    con.commit()
