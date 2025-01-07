import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY

def connect_to_s3():
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id = AWS_ACCESS_KEY_ID,
            aws_secret_access_key = AWS_ACCESS_KEY
        )
        return s3_client
    except Exception as e:
        print(e)

def create_bucket_if_not_exist(s3_client,
                               bucket_name: str):
    try: 
        response = s3_client.head_bucket(Bucket=bucket_name)
    except ClientError:
        try: 
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} created.")
        except ClientError as e:
            print(f"Error creating bucket: {e}")
    
# def create_folder_if_not_exist(s3_client, bucket_name, folder_name):
#     try:
#         s3_client.head_object(Bucket=bucket_name, Key=folder_name)
#         print(f"Folder {folder_name} already exists in bucket {bucket_name}.")
#     except ClientError as e:
#         if e.response['Error']['Code'] == '404':
#             try:
#                 s3_client.put_object(Bucket=bucket_name, Key=folder_name)
#                 print(f"Folder {folder_name} created in bucket {bucket_name}.")
#             except ClientError as e: print(f"Error creating folder: {e}")
#         else:
#             print(f"Error checking folder: {e}")   

def upload_to_s3(s3_client, file_name, bucket_name, folder_name):
    try:
        s3_client.upload_file(Filename=file_name, Bucket=bucket_name, Key=f'{file_name}')
    except ClientError as e:
        print(f"Error uploading file: {e}")
    except FileNotFoundError:
        print(f"File {file_name} not found.")
    except NoCredentialsError:
        print("Credentials not available")