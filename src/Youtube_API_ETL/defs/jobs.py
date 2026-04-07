import dagster as dg
from .assets import youtube_video_stats


asset_define = dg.define_asset_job(
        name='YouTube_stat_job',
        selection=dg.AssetSelection.assets(youtube_video_stats)
)
