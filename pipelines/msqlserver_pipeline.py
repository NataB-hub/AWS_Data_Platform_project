import pandas as pd

from etls.sqlserver_etls import connect_to_sql_server, fetch_table_data, load_data_to_csv
from utils.constants import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER, OUTPUT_PATH

def sql_server_pipeline(file_postfix: str,
                        schema_tables: list):
    #connecting to microsoft sql server
    sql_conn = connect_to_sql_server(server = SQL_SERVER,
                                     database = SQL_DATABASE,
                                     username=SQL_USERNAME,
                                     password=SQL_PASSWORD,
                                     driver=SQL_DRIVER)
    #extraction
    files_list = []
    for schema_table in schema_tables:
        schema = schema_table['schema']
        for table_name in schema_table['tables']:
            data = fetch_table_data(sql_conn, schema, table_name)
            #loading to csv
            if data is not None:
                file_path = f'{OUTPUT_PATH}/{schema}_{table_name}_{file_postfix}.csv'
                load_data_to_csv(data, file_path)
                files_list.append(file_path)
    return files_list
    #transformation

    
