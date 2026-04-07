import dagster as dg
from ..resources import neon_postgres_resource
from .sensors import youtube_stats
from .jobs import asset_define
from .assets import youtube_video_stats

defs = dg.Definitions(
    assets=[youtube_video_stats],
    jobs=[asset_define],
    resources ={"postgres_db": neon_postgres_resource},
    sensors = [youtube_stats]
)