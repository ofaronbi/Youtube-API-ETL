import dagster as dg
import os
from datetime import date, datetime, timezone
from ..helper import load_json, str_to_date



runtime = int(os.getenv("INTERVAL", 150))


@dg.sensor(job_name='YouTube_stat_job', minimum_interval_seconds=runtime, default_status=dg.DefaultSensorStatus.RUNNING)
def youtube_stats(context: dg.SensorEvaluationContext):
    today = date.today()
    path = './playlist_id.json'
    data = load_json(path)
    last_call_str = data.get('call_date', '1900-01-01')
    last_call_date = str_to_date(last_call_str)
    run_key = datetime.now(timezone.utc).isoformat()
    if today > last_call_date:
        yield dg.RunRequest(
            run_key= run_key,
            run_config={
                "ops": {
                    "YouTube_video_statistic": {
                        "ops": {
                            "get_playlist_id": {
                                "config": {"current_date": str(today)}
                            }
                        }
                    }
                }
            }
        )
    
    yield dg.SkipReason(f"Data is up to date {today}")
    context.update_cursor(run_key)