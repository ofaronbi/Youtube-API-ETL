import dagster as dg
from .ops import youtube_stats


youTube_stat_job = dg.define_asset_job(
    name='YouTube_stat_job',
    selection=dg.AssetSelection.assets(youtube_stats)
)