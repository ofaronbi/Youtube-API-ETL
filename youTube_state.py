import requests, json, os
from  datetime import date, datetime
from dotenv import load_dotenv


load_dotenv(dotenv_path="./.env")

YOUR_API_KEY = os.getenv("YOUR_API_KEY")
CHANNEL_HANDLER = os.getenv("CHANNEL_HANDLER")
MAX_RESULT = os.getenv("MAX_RESULT")


def json_data(file, url, append_data=None, current_date=None):
    try:
        response = requests.get(url)
        response.raise_for_status()
        new_data = response.json()
        if append_data and current_date:
            new_data['call_date'] = str(current_date)
        file.seek(0)
        file.truncate()
        json.dump(new_data, file, indent=4)
        data = new_data 
    except Exception as e:
        raise e


def get_playlist_id(current_date):
    with open('./playlist_id.json', 'r+') as file:
        data = json.load(file)
        last_call_date = datetime.strptime(data['call_date'], '%Y-%m-%d').date()
        if current_date > last_call_date:
            url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLER}&key={YOUR_API_KEY}'
            json_data(file, url, True, current_date)
            
    if data.get('items'):
        channel_playlistid = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return channel_playlistid, last_call_date
    else:
        return None, None


def playlist(current_date):
    playlist_id,last_call_date  = get_playlist_id(current_date)
    if playlist_id:
        with open("./playlist.json", "r+") as file:
            if current_date > last_call_date:
                url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={MAX_RESULT}&playlistId={playlist_id}&key={YOUR_API_KEY}"
                json_data(file, url)
                
            else:
                print("Date is still current in playlist.")
                data = json.load(file)
                

def get_playlist_video_ids(current_date):
    playlist_id, last_call_date = get_playlist_id(current_date)
    if not playlist_id:
        print("No playlist ID found.")
        return None
    
    playlist_path = "./playlist.json"
    
    if current_date > last_call_date:
        print("Fetching new playlist data...")
        all_video_details = []
        page_token = None
            
        while True:
            params = {
                "part": "contentDetails",
                "maxResults": MAX_RESULT, 
                "playlistId": playlist_id,
                "key": YOUR_API_KEY
            }
            if page_token:
                params["pageToken"] = page_token
            
            response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params=params)
            response.raise_for_status()
            data = response.json()
            
            for item in data.get("items", []):
                all_video_details.append(item["contentDetails"])
                
            page_token = data.get("nextPageToken")
                    
            if not page_token:
                new_data = {"Video Details": all_video_details}
                with open(playlist_path, "w") as file:
                    json.dump(new_data, file, indent=4)
                return new_data
    else:
        print("Loading playlist from cache.")
        with open(playlist_path, "r") as file:
            return json.load(file)


def youTube_stats():
    current_date = date.today()
    data = get_playlist_video_ids(current_date)
    print(f"Retrieved {len(data.get('Video Details', []))} videos.")



if __name__ == "__main__":   
    youTube_stats()