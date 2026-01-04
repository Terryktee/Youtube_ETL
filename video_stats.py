import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()
Channel_handler = 'MrBeast'

def get_playlist_id():
    try:
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={Channel_handler}&key={os.getenv("API_KEY")}'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        channel_items = data["items"][0]
        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        print(f'Playlist ID : {channel_playlistId}')
        
    except requests.exceptions.RequestException as e:
        raise e

if __name__ == "__main__":
   get_playlist_id()