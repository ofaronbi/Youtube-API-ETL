import dagster as dg
import os
from datetime import date, datetime, timezone
from ..helper import str_to_date


runtime = int(os.getenv("INTERVAL", 150))

@dg.sensor(
    job_name='YouTube_stat_job', 
    minimum_interval_seconds=runtime, 
    default_status=dg.DefaultSensorStatus.RUNNING, 
    required_resource_keys={'postgres_db'}
    )
def youtube_stats(context: dg.SensorEvaluationContext):
    
    db_conn = context.resources.postgres_db
    
    with db_conn.cursor() as conn:
        conn.execute("""
                     SELECT call_date FROM PLAYLIST_ID ORDER BY call_date DESC LIMIT 1
                     """)
        row = conn.fetchone()

    if not row:
        raise Exception("PLAYLIST_ID table is empty.")
    
    today = date.today()
    last_call_date = str_to_date(row[0])
    
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
    else:
        yield dg.SkipReason(f"Data is up to date {today}")
        
    context.update_cursor(run_key)