import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))

#SQL Server
SQL_SERVER = parser.get('sql_database', 'sql_server')
SQL_DATABASE = parser.get('sql_database', 'sql_database')
SQL_USERNAME = parser.get('sql_database', 'sql_username')
SQL_PASSWORD = parser.get('sql_database', 'sql_password')
SQL_DRIVER = parser.get('sql_database', 'sql_driver')

#FILE PATHS
OUTPUT_PATH = parser.get('file_paths', 'output_path')

#AWS
AWS_ACCESS_KEY_ID = parser.get('aws', 'aws_access_key_id')
AWS_ACCESS_KEY = parser.get('aws', 'aws_access_key')
BUCKET_NAME = parser.get('aws', 'bucket_name')
BUCKET_RAW_FOLDER = parser.get('aws', 'bucket_raw_folder')
