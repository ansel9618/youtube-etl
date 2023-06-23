import requests
import math
import json
from datetime import date

from airflow.models import Variable

# Getting Global vars

API_KEY = Variable.get("API_KEY")
CHANNEL_ID = Variable.get("CHANNEL_ID")
# maxResults is the maximum number of videos the YT API shows per page. This is equal to 50 according to documentation
maxResults = 50


# Total number of videos of the channel and UploadID
def videos_count():
    try:
        URL_outer = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails%2Cstatistics&id={CHANNEL_ID}&fields=items.contentDetails.relatedPlaylists.uploads%2Citems.statistics.videoCount&key={API_KEY}"
        r = requests.get(url=URL_outer)
        params = r.json()

        UploadID = params["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        videos_count = int(params["items"][0]["statistics"]["videoCount"])

        return videos_count, UploadID

    except:
        if r.status_code != 200:
            raise Exception(r.json())
        else:
            if len(r.json()) == 0:
                raise ValueError("Incorrect Channel_ID")


# Below function is needed because you cannot loop through each page of a youtube channel.
# Create a TOKENS list with '' set as the first element
# This is done since the 1st page is obtained when URL doesn't have 'pageToken={TOKEN}' argument
def tokens():
    # First element of TOKENS list is ""
    TOKENS = [""]

    # Get total number of videos from videos_count function
    video_count, UploadID = videos_count()
    max_tokens = math.ceil(video_count / maxResults)

    try:
        for token_no in range(0, max_tokens - 1):
            # URL_inner created using the 'Overview of Youtube API' from External Docs under the Search section - ordered by date (DESC)
            if token_no == 0:
                URL_inner = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults={maxResults}&playlistId={UploadID}&key={API_KEY}"
            else:
                URL_inner = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults={maxResults}&pageToken={TOKEN}&playlistId={UploadID}&key={API_KEY}"
            r = requests.get(url=URL_inner)
            vids = r.json()
            TOKEN = vids["nextPageToken"]
            TOKENS.append(TOKEN)

    except:
        if r.status_code != 200:
            raise Exception(r.json())

    return TOKENS


# Function that extracts the 6 important variables for this analysis:
# Video ID, Video Title, Upload Date, Video Views, Likes Count, Comments Count
def video_stats():
    try:
        URL_outer = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails%2Cstatistics&id={CHANNEL_ID}&fields=items.contentDetails.relatedPlaylists.uploads%2Citems.statistics.videoCount&key={API_KEY}"

        r = requests.get(url=URL_outer)
        params = r.json()
        UploadID = params["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        # Get TOKENS list from tokens function
        TOKENS = tokens()

        # Creating empty list to fill with the data from YT API
        vid_data = []

        for token in TOKENS:
            if token == "":
                URL = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults={maxResults}&playlistId={UploadID}&key={API_KEY}"
            else:
                URL = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults={maxResults}&pageToken={token}&playlistId={UploadID}&key={API_KEY}"
            r = requests.get(url=URL)
            vids = r.json()

            for vid in vids["items"]:
                video_id = vid["snippet"]["resourceId"]["videoId"]
                video_title = vid["snippet"]["title"]
                upload_date = vid["snippet"]["publishedAt"]

                # To get the stats, videos resource was used in combination with the video_id found from the outer API request
                URL_stats = f"https://youtube.googleapis.com/youtube/v3/videos?part=statistics&id={video_id}&key={API_KEY}"
                r = requests.get(url=URL_stats)
                vids_stats = r.json()["items"][0]["statistics"]

                video_views = vids_stats["viewCount"]
                likes_count = vids_stats["likeCount"]
                comments_count = vids_stats["commentCount"]

                data = {
                    "Video_ID": video_id,
                    "Video_Title": video_title,
                    "Upload_Date": upload_date,
                    "Video_Views": video_views,
                    "Likes_Count": likes_count,
                    "Comments_Count": comments_count,
                }

                vid_data.append(data)

        # Serializing json
        data_json = json.dumps(vid_data, indent=4)

        # Writing to YT_data.json
        with open(f"/opt/airflow/include/YT_data_{date.today()}.json", "w") as outfile:
            outfile.write(data_json)

    # status code 403 occurs when you have reached max API pull quota
    # status code 400 occurs when an error related to the API key occurs
    except:
        if r.status_code != 200:
            raise Exception(r.json())
        else:
            if len(r.json()) == 0:
                raise ValueError("Incorrect Channel_ID")


def load_data():
    data = open(f"/opt/airflow/include/YT_data_{date.today()}.json")
    YT_data = json.load(data)

    return YT_data
