from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from datetime import timedelta, datetime
from api.video_stats import get_playlist_id, get_videos_ids, batch_list, extract_video_data, save_to_json
from api.datawarehouse.dwh import staging_table,core_table
from api.dataquality.soda import yt_elt_data_quality

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

staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to fetch YouTube video stats and save to JSON",
    schedule="0 14 * * * " ,
    catchup=False,
) as dag_produce :
    playlist_id=get_playlist_id()
    video_ids = get_videos_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="Trigger_update_db",
        trigger_dag_id="update_db",

    )
    playlist_id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_db


with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process json file and insert data into both staging and core schema ",
    schedule=None,
    catchup=False,
) as dag_update :
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )
    update_staging >> update_core >> trigger_data_quality


with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check data quality on both layers in the db",
    schedule=None ,
    catchup=False,
    
) as dag_quality:
    soda_validate_staging= yt_elt_data_quality(staging_schema)
    soda_validate_core= yt_elt_data_quality(core_schema)
    

    soda_validate_staging >>soda_validate_core
