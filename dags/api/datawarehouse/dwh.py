from .data_utils import get_conn_cursor,close_conn_cursor,create_schema,create_table,get_video_ids
from .data_loading import load_data
from .data_modifications import insert_rows,update_rows,delete_rows
from .data_transformation import trasform_data
from airflow.decorators import task


import logging


logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():
    schema = 'staging'

    conn,cur = None,None

    try:
        conn,cur = get_conn_cursor()
        YT_data = load_data()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur,schema)

        for row in YT_data:

            if len(table_ids) == 0 :
                insert_rows(cur,conn,schema,row)
            else:
                if row['video_id'] in table_ids:
                    update_rows(cur,conn,schema,row)
                else:
                    insert_rows(cur,conn,schema,row)
        ids_in_json = {row['video_id'] for row in YT_data}
        ids_to_delete = set(table_ids)-ids_in_json

        if ids_to_delete:
            delete_rows(cur,conn,schema,ids_to_delete)
        logger.info(f"{schema} table updated completed")
    except Exception as e:
        logger.error(f"An error occured during the update of {schema} table {e}")
    finally:
        if conn and cur :
            close_conn_cursor(conn,cur)

@task
def core_table():

    schema="core"
    conn,cur=None,None

    try:
        conn,cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids=get_video_ids(cur,schema)
        current_video_ids = set()
        cur.execute(f"SELECT * FROM staging.{table};")
        rows=cur.fetchall()
        
        for row in rows:
            current_video_ids.add(row["Video_ID"])
            if len(table_ids) == 0:
                trasformed_row=trasform_data(row)
                insert_rows(cur,conn,schema,trasformed_row)
            else:
                trasformed_row = trasform_data(row)

                if trasformed_row['Video_ID'] in table_ids:
                    update_rows(cur,conn,schema,trasformed_row)
                else:
                    insert_rows(cur,conn,schema,trasformed_row)
        ids_to_delete=set(table_ids)-current_video_ids

        if ids_to_delete:
            delete_rows(cur,conn,schema,ids_to_delete)

        logger.info(f'{schema} table updated completed')
    
    except Exception as e :
        logger.error(f'AN error occurred during the update of {schema} table:{e}')

    finally:
        if conn and cur:
            close_conn_cursor(conn,cur)

