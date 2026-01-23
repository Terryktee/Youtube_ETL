import requests

import json

from airflow.decorators import task
from datetime import date
from airflow.models import Variable



#import os
#from dotenv import load_dotenv
#load_dotenv()

#CHANNEL_HANDLE = "MrBeast"
MAX_RESULTS = 50
API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")

@task
def get_playlist_id():
    url = (
        "https://youtube.googleapis.com/youtube/v3/channels"
        f"?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
    )
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    playlist_id = data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    print(f"Playlist ID: {playlist_id}")
    return playlist_id

@task
def get_videos_ids(playlist_id):
    video_ids = []
    page_token = None

    while True:
        url = (
            "https://youtube.googleapis.com/youtube/v3/playlistItems"
            f"?part=contentDetails&playlistId={playlist_id}"
            f"&maxResults={MAX_RESULTS}&key={API_KEY}"
        )

        if page_token:
            url += f"&pageToken={page_token}"

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        for item in data["items"]:
            video_ids.append(item["contentDetails"]["videoId"])

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    print(f"Fetched {len(video_ids)} video IDs")
    return video_ids

@task
def batch_list(items, batch_size):
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]

@task
def extract_video_data(video_ids: list[str]):
    extracted_data = []

    def batch_list(video_id_lst,batch_size):
        for video_id in range(0,len(video_id_lst),batch_size):
            yield video_id_lst[video_id:video_id+batch_size]

    try:
        for batch in batch_list(video_ids, MAX_RESULTS):
            video_ids_str = ",".join(batch)
            url = (
                "https://youtube.googleapis.com/youtube/v3/videos"
                f"?part=snippet,statistics,contentDetails"
                f"&id={video_ids_str}&key={API_KEY}"
            )

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data["items"]:
                video_data = {
                    "video_id": item["id"],
                    "title": item["snippet"]["title"],
                    "published_at": item["snippet"]["publishedAt"],
                    "view_count": item["statistics"].get("viewCount", 0),
                    "like_count": item["statistics"].get("likeCount", 0),
                    "comment_count": item["statistics"].get("commentCount", 0),
                    "duration": item["contentDetails"]["duration"],
                }
                extracted_data.append(video_data)
            return extracted_data
    except requests.exceptions.RequestException as e:
        raise e
        
    

@task
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4 , ensure_ascii=False)

if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_videos_ids(playlist_id)

    print("Extracting video data...")
    video_data = extract_video_data(video_ids)

    print(json.dumps(video_data[:5], indent=4))  # print first 5 only
    save_to_json(video_data)
    print("Data saved to JSON file.")
