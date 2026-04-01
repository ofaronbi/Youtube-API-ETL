import dagster as dg
import requests
from ..helper import *



YOUR_API_KEY = dg.EnvVar("YOUR_API_KEY")
CHANNEL_HANDLER = dg.EnvVar("CHANNEL_HANDLER")
MAX_RESULT = dg.EnvVar("MAX_RESULT")


class CURRENTDATE(dg.Config):
    current_date: str
        


@dg.op
def get_playlist_id(date):
    path = './playlist_id.json'
    data = load_json(path)
    
    # Check if we need to refresh (if date is missing or old)
    last_call_str = data.get('call_date', '1900-01-01')
    last_call_date = str_to_date(last_call_str)
    current_date = str_to_date(date)

    if current_date > last_call_date:
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLER}&key={YOUR_API_KEY}'
        response = requests.get(url)
        response.raise_for_status()
        new_data = response.json()
        
        if 'items' in new_data:
            new_data['call_date'] = str(current_date)
            save_json(path, new_data)
            return new_data['items'][0]['contentDetails']['relatedPlaylists']['uploads'], last_call_date
    
    return current_date, data['items'][0]['contentDetails']['relatedPlaylists']['uploads'], last_call_date


@dg.op
def get_playlist_video_ids(current_date, playlist_id, last_call_date):
    path = "./playlist.json"
    
    if current_date > last_call_date:
        print("Fetching fresh playlist items...")
        all_videos = []
        page_token = None
        
        while True:
            params = {
                "part": "contentDetails",
                "maxResults": MAX_RESULT,
                "playlistId": playlist_id,
                "key": YOUR_API_KEY,
                "pageToken": page_token
            }
            response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params=params)
            response.raise_for_status()
            data = response.json()
            
            all_videos.extend([item["contentDetails"]["videoId"] for item in data.get("items", [])])
            page_token = data.get("nextPageToken")
            if not page_token: break
            
        save_json(path, {"video_ids": all_videos})
        return all_videos
    
    return load_json(path).get("video_ids", [])


@dg.op
def fetch_full_video_details(video_ids):
    print(f"Fetching details for {len(video_ids)} videos...")
    all_details = []
    
    for index in range(0, len(video_ids), 50):
        chunk_id = video_ids[index:index+50]
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(chunk_id),
            "key": YOUR_API_KEY
        }
        response = requests.get("https://www.googleapis.com/youtube/v3/videos", params=params)
        response.raise_for_status()
        data = response.json()
        
        for item in data.get("items", []):
            statistics = item.get("statistics", {})
            video_data = {
                "video_id" : item["id"],
                "channelTitle" : item["snippet"]["channelTitle"],
                "title" : item["snippet"]["title"],
                "publishedAt" : item["snippet"]["publishedAt"],
                "description" : item["snippet"]["description"],
                "duration" : format_duration(item["contentDetails"]["duration"]),
                "viewCount" : statistics.get("viewCount", "0"),
                "likeCount" : statistics.get("likeCount", "0"),
                "commentCount" : statistics.get("commentCount", "0")
            }
            
            all_details.append(video_data)
    
    save_json("./video_data.json", all_details)
    return all_details


@dg.graph_asset(kinds=['Python', 'PostgreSQL'], owners=['team:Opeyemi-Faronbi'], group_name='YouTube_stats')
def YouTube_stats(config: CURRENTDATE):
    return fetch_full_video_details(get_playlist_video_ids(get_playlist_id(config.current_date)))
    # today = date.today()
    # try:
    #     today, playlist_id, last_date = get_playlist_id(today)
    #     video_ids = get_playlist_video_ids(today, playlist_id, last_date)
        
    #     if today > last_date:
    #         fetch_full_video_details(video_ids)
    #         print("Data updated successfully.")
    #     else:
    #         print("Cache is up to date.")
            
    # except Exception as e:
    #     print(f"Error: {e}")