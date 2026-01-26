import logging


logger = logging.getLogger(__name__)
table = "yt_api"


def insert_rows(cur,conn,schema,row):
    try:
        if schema == "staging":

            video_id = "video_id"

            cur.execute(
                f""" INSERT INTO {schema}.{table}("Video_ID","Video_Title","Upload_Date","Duration","Video_Views","Likes_Count","Comment_Count")
                VALUES(%(video_id)s,%(title)s,%(published_at)s,%(duration)s,%(view_count)s,%(like_count)s,%(comment_count)s);
                """,row
            )
        else:
            video_id = "Video_ID"
            cur.execute(
                f""" INSERT INTO {schema}.{table}("Video_ID","Video_Title","Upload_Date","Duration","Video_Type","Video_Views","Likes_Count","Comment_Count")
                VALUES(%(Video_ID)s,%(Video_Title)s,%(Upload_Date)s,%(Duration)s,%(Video_Type)s,%(Video_Views)s,%(Likes_Count)s,%(Comment_Count)s);
                """,
                row
                ,
            )
        conn.commit()
        logger.info(f"Inserted row with {video_id}: {row[video_id]}")
    except Exception as e:
        logger.error(f"Error inserting row with {video_id}: {row[video_id]} - {e}")
        raise e
    
def update_rows(cur,conn,schema,row):
    try:
        if schema == "staging":
            video_id='video_id'
            upload_date='publichedAt'
            video_title='title'
            video_views='viewCount'
            likes_count='likeCount'
            comments_count='commentCount'

        else:
            video_id='Video_ID'
            upload_date='Upload_Date'
            video_title='Video_Title'
            video_views='Video_Views'
            likes_count = 'Like_Count'
            comments_count = 'Comments_Count'

        cur.execute(
            f"""
            UPDATE {schema}.{table}
            SET "video_Title" = %({video_title})s,
                "Video_Views" = %({video_views})s,
                "likes_Count"=%({likes_count})s,
                "Comments_Count"= %({comments_count})s,
            WHERE "video_ID" = %({video_id})s AND "UPLOAD_DATE" = %({upload_date})s;
        """,row,
        )
        conn.commit()

        logger.info(f"Updated row with video_ID:{row[video_id]}")

    except Exception as e:
        logger.error(f"Error updating row with VIdeo_ID:{row[video_id]- {e}}")
        raise e
def delete_rows(cur,conn,schema,ids_to_delete):

    try:
        id_to_delete = f""" ({','.join(f"'{id}'" for id in ids_to_delete)})"""
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN {ids_to_delete};
            """
        )

        conn.commit()
        logger.info(f"Deleted rows with Video_IDs:{ids_to_delete}")

    except Exception as e:
        logger.error(f"Error deleting rows with Video_IDs{ids_to_delete}-{e}")
        raise e