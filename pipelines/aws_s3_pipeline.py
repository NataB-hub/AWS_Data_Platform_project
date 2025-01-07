from etls.aws_etl import connect_to_s3, create_bucket_if_not_exist, upload_to_s3

from utils.constants import BUCKET_NAME, BUCKET_RAW_FOLDER

def upload_s3_pipeline(ti):
    file_paths = ti.xcom_pull(task_ids='sql_extraction', key='return_value')

    s3 = connect_to_s3()
    create_bucket_if_not_exist(s3,
                               BUCKET_NAME)
    # create_folder_if_not_exist(s3,
    #                            BUCKET_NAME,
    #                            BUCKET_RAW_FOLDER)
    for file_path in file_paths:
        upload_to_s3(s3,
                     file_path,
                     BUCKET_NAME,
                     BUCKET_RAW_FOLDER)