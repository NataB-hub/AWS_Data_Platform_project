from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
import pandas as pd


def connect_to_sql_server(server:str,
                            database:str,
                            username:str,
                            password:str,
                            driver:str):
    
    try:
        connection_string = (
            f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"
        )
        engine = create_engine(connection_string)
        print("Connection successful.")
        return engine
    except Exception as e:
        print(f"Error connection to SQL Server: {e}")
        return None
    
def fetch_table_data(engine,
                     schema:str,
                     table_name:str,
                     columns="*",
                     filters=None):
    try:
        full_table_name = f'{schema}.{table_name}'
        query = f'SELECT {columns} FROM {full_table_name}'
        if filters:
            query += f'WHERE {filters}'
        with engine.connect() as conn:
            result = pd.read_sql(sql=query, con=conn.connection)
        print(f'Data successfully retrieved from {full_table_name}.')
        return result
    
    except ProgrammingError as e:
        print(f'ProgrammingError fetching data from {full_table_name}: {e}')

        try:
            schema_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
            """
            with engine.connect() as conn:
                schema_df = pd.read_sql(sql=schema_query, con=conn.connection)
            
            issue_columns = schema_df[schema_df['DATA_TYPE'] == 'hierarchyid']['COLUMN_NAME'].tolist()
            print(f"Identified problematic columns: {issue_columns}")
            columns_to_select = ", ".join(
                [col for col in schema_df['COLUMN_NAME'] if col not in issue_columns]
            )
            if not columns_to_select:
                print("No valid columns to select from the table.")
                return None
            query = f"SELECT {columns_to_select} FROM {full_table_name}"
            with engine.connect() as conn:
                result = pd.read_sql(sql=query, con=conn.connection)

            print(f"Data successfully retrieved from {full_table_name}, excluding issue columns.")
            return result
        except Exception as e:
            print(f"Error fetching schema from {full_table_name}: {e}")
            return None
    
    except Exception as e:
        print(f'Error fetching data from {full_table_name}: {e}')
        return None
    
def load_data_to_csv(data: pd.DataFrame,
                     path: str):
    data.to_csv(path, index=False)
    print(f'Data successfully saved to {path}')