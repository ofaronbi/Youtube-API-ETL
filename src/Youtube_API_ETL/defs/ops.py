import dagster as dg
import requests
import os
from ..helper import *


YOUR_API_KEY = os.getenv("YOUR_API_KEY")
CHANNEL_HANDLER = os.getenv("CHANNEL_HANDLER")
MAX_RESULT = int(os.getenv("MAX_RESULT", 50))


class CURRENTDATE(dg.Config):
    current_date: str
    

@dg.op(out={'playlist_id': dg.Out()},
    required_resource_keys={'postgres_db'}
)
def get_playlist_id(context:dg.OpExecutionContext, config:CURRENTDATE):
    
    db_conn = context.resources.postgres_db
    current_date = str_to_date(config.current_date)
        
    context.log.info("Fetching new data from YouTube API...")
    url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLER}&key={YOUR_API_KEY}'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    uploads_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    channel_id = data['items'][0]['id']
    
    truncate_table(db_conn, 'PLAYLIST_ID')

    with db_conn.cursor() as conn:
        conn.execute("""
                INSERT INTO PLAYLIST_ID(channel_id, uploads_id, call_date)
                VALUES(%s, %s, %s)
                """, (channel_id, uploads_id, current_date))

    yield dg.Output(uploads_id, 'playlist_id')


@dg.op(required_resource_keys={'postgres_db'})
def get_playlist_video_ids(context:dg.OpExecutionContext, playlist_id):
    
    db_conn = context.resources.postgres_db
    
    truncate_table(db_conn, 'PLAYLIST_VIDEO_ID')
    context.log.info("Fetching fresh playlist items...")
    
    page_token = None
    
    with db_conn.cursor() as conn:
        while True:
            params = {
                "part": "contentDetails",
                "maxResults": MAX_RESULT,
                "playlistId": playlist_id,
                "key": YOUR_API_KEY,
                "pageToken": page_token
            }
            response = requests.get(
                "https://www.googleapis.com/youtube/v3/playlistItems", params=params)
            response.raise_for_status()
            data = response.json()
            
            for item in data.get("items", []):
                video_di = item["contentDetails"]["videoId"]
                conn.execute("""
                            INSERT INTO PLAYLIST_VIDEO_ID(video_id)
                            VALUES(%s)
                        """, (video_di,))

            page_token = data.get("nextPageToken")
            if not page_token:
                break
                
        context.log.info("Playlist video IDs saved to PLAYLIST_VIDEO_ID Database successfully.")
                
    with db_conn.cursor() as conn:
        conn.execute("""
                     SELECT video_id FROM PLAYLIST_VIDEO_ID
                     """)
        rows = conn.fetchall()
    
    return [row[0] for row in rows]


@dg.op(required_resource_keys={"postgres_db"})
def fetch_full_video_details(context:dg.OpExecutionContext, video_ids):
    db_conn = context.resources.postgres_db
    try:
        context.log.info(f"Fetching details for {len(video_ids)} videos...")
        truncate_table(db_conn, "YOUTUBE_VIDEOS")

        for index in range(0, len(video_ids), MAX_RESULT):
            chunk_id = video_ids[index:index + MAX_RESULT]
            params = {
                "part": "snippet,statistics,contentDetails",
                "id": ",".join(chunk_id),
                "key": YOUR_API_KEY
            }
            response = requests.get(
                "https://www.googleapis.com/youtube/v3/videos", params=params)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                statistics = item.get("statistics", {})
                video_id = item["id"]
                channel_title = item["snippet"]["channelTitle"]
                title = item["snippet"]["title"]
                published_at = item["snippet"]["publishedAt"]
                description = item["snippet"]["description"]
                duration = format_duration(item["contentDetails"]["duration"])
                view_count = statistics.get("viewCount", "0")
                like_count = statistics.get("likeCount", "0")
                comment_count = statistics.get("commentCount", "0")
                image_url = item["snippet"]["thumbnails"]["high"]["url"]
                
                with db_conn.cursor() as conn:
                    conn.execute("""
                        INSERT INTO YOUTUBE_VIDEOS(video_id, channel_title, title, published_at, description, duration, image_url)
                        VALUES(%s, %s, %s, %s, %s, %s, %s)
                    """, (video_id, channel_title, title, published_at, description, duration, image_url))
                    
                    conn.execute("""
                        INSERT INTO YOUTUBE_VIDEO_STATS(video_id, view_count, like_count, comment_count)
                        VALUES(%s, %s, %s, %s)
                    """, (video_id, int(view_count), int(like_count), int(comment_count)))
                    
        context.log.info("All tables ready.")
    except:
        raise dg.DagsterError

    

