import dagster as dg
from .ops import *


@dg.graph_asset(
    name='YouTube_video_statistic', 
    kinds=['Python', 'PostgreSQL'], 
    owners=['team:Opeyemi-Faronbi'], 
    group_name='YouTube_stats'
    )
def youtube_video_stats():
    playlist_id = get_playlist_id()
    video_ids = get_playlist_video_ids(playlist_id)
    return fetch_full_video_details(video_ids)
