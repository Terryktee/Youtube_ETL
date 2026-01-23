from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table='yt_api'

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id='postgres_db_yt_elt',database='elt_db')
    conn=hook.get_conn()
    cur= conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(conn,cur):
    cur.close()
    conn.close()

def create_schema(schema):
    conn,cur=get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(schema_sql)
    conn.commit()

    close_conn_cursor(conn,cur)

def create_table(schema):

    conn,cur = get_conn_cursor()
    if schema == 'staging':
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_Title" VARCHAR NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" VARCHAR NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comment_Count" INT
        );
        """

    else:
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_ID" VARCHAR(14) PRIMARY KEY NOT NULL,
            "Video_Title" VARCHAR NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" VARCHAR NOT NULL,
            "Video_Type" VARCHAR NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comment_Count" INT
        );
        """
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn,cur)

def get_video_ids(cur,schema):
    
    video_id_sql = f'SELECT "Video_ID" FROM {schema}.{table};'
    cur.execute(video_id_sql)
    video_ids = [row["Video_ID"] for row in cur.fetchall()]

    return video_ids
