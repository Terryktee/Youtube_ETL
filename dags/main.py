from airflow import DAG
import pendulum
from datetime import timedelta, datetime
from api.video_stats import get_playlist_id, get_videos_ids, batch_list, extract_video_data, save_to_json
from api.datawarehouse.dwh import staging_table,core_table

local_tz = pendulum.timezone("Africa/Harare")
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email":"terryktee@gmail.com",
    "max_active_runs": 1,
    "dagrun_timeout":timedelta(hours=1),
    "start_date": datetime(2026, 1, 20, tzinfo=local_tz),
    #retries: 1,
    #retry_delay: timedelta(minutes=5),
}

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to fetch YouTube video stats and save to JSON",
    schedule="0 14 * * * " ,
    catchup=False,
) as dag :
    playlist_id=get_playlist_id()
    video_ids = get_videos_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    playlist_id >> video_ids >> extract_data >> save_to_json_task


with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process json file and insert data into both staging and core schema ",
    schedule="0 15 * * * " ,
    catchup=False,
) as dag :
    update_staging = staging_table()
    update_core = core_table()

    update_staging >> update_core 
