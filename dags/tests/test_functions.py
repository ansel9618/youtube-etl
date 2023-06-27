import requests
import math

from airflow.models import Variable, Connection

from api_script.video_stats import videos_count, tokens


# Making sure that variables API_KEY, CHANNEL_ID & JOINED_DATE exist
def test_variables():
    assert Variable.get("API_KEY") is not None
    assert Variable.get("CHANNEL_ID") is not None
    assert Variable.get("JOINED_DATE") is not None


# Making sure that connection with conn_id = POSTGRES_DB_YT_ETL exists
def test_connection():
    assert (
        Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ETL") is not None
    )


# Test that first element in TOKENS list is ""
# and that the length of tokens list == math.ceil(video_count / maxResults)
def test_tokens_list():
    video_count = videos_count()[0]
    maxResults = 50

    assert tokens()[0] == ""
    assert len(tokens()) == math.ceil(video_count / maxResults)


# Making sure outer URL gives status code 200
def test_videos_count(api_key, channel_id):
    URL_outer = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails%2Cstatistics&id={channel_id}&fields=items.contentDetails.relatedPlaylists.uploads%2Citems.statistics.videoCount&key={api_key}"
    r = requests.get(url=URL_outer)

    assert r.status_code == 200
