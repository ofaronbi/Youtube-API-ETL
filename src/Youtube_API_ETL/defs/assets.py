import dagster as dg
from .ops import *


@dg.graph_asset(name='YouTube_video_statistic',kinds=['Python', 'PostgreSQL'], owners=['team:Opeyemi-Faronbi'], group_name='YouTube_stats')
def youtube_stats():
    current_date, playlist_id, last_call_date = get_playlist_id()
    video_ids = get_playlist_video_ids(current_date, playlist_id, last_call_date)
    return fetch_full_video_details(video_ids)