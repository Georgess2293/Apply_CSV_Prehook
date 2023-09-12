import os
from database_handler import execute_query, create_connection, close_connection,return_data_as_df, return_create_statement_from_df
from lookups import ErrorHandling, PreHookSteps, SQLTablesToReplicate, InputTypes, SourceName
from logging_handler import show_error_message

def execute_sql_folder(db_session, sql_command_directory_path):
    sql_files = [sqlfile for sqlfile in os.listdir(sql_command_directory_path) if sqlfile.endswith('.sql')]
    sorted_sql_files =  sorted(sql_files)
    for sql_file in sorted_sql_files:
        with open(os.path.join(sql_command_directory_path,sql_file), 'r') as file:
            sql_query = file.read()
            return_val = execute_query(db_session= db_session, query= sql_query)
            if not return_val == ErrorHandling.NO_ERROR:
                raise Exception(f"{PreHookSteps.EXECUTE_SQL_QUERY.value} = SQL File Error on SQL FILE = " +  str(sql_file))
    
def return_tables_by_schema(schema_name):
    schema_tables = list()
    tables = [table.value for table in SQLTablesToReplicate]
    for table in tables:
        if table.split('.')[0] == schema_name:
            schema_tables.append(table)
    return schema_tables

def return_csv_file_directories(csv_folder_directory_path):
    csv_files_paths=[os.path.join(csv_folder_directory_path,csv_file) for csv_file in os.listdir(csv_folder_directory_path) if csv_file.endswith('.csv')]
    return csv_files_paths

def get_table_name_from_csv(csv_file_directory_path):
    table_name=(csv_file_directory_path.split('\\')[1]).split('.')[0].replace(' ','_')
    return table_name

def create_sql_staging_tables(db_session, source_name):
    tables = return_tables_by_schema(source_name)
    for table in tables:
        staging_query = f"""
                SELECT * FROM {source_name}.{table} LIMIT 1
        """
        staging_df = return_data_as_df(db_session= db_session, input_type= InputTypes.SQL, file_executor= staging_query)
        dst_table = f"stg_{source_name}_{table}"
        create_stmt = return_create_statement_from_df(staging_df, 'dw_reporting', dst_table)
        execute_query(db_session=db_session, query= create_stmt)

def create_csv_staging_tables(db_session,source_name):
    files = return_csv_file_directories(source_name)
    for file in files:
        staging_df = return_data_as_df(input_type= InputTypes.CSV, file_executor= file)
        staging_df.columns=staging_df.columns.str.replace(" |-","_",regex=True)
        dst_table = get_table_name_from_csv(file)
        create_stmt = return_create_statement_from_df(staging_df, 'dw_reporting', dst_table)
        execute_query(db_session=db_session, query= create_stmt)
        

def execute_prehook(sql_command_directory_path = './SQL_Commands'):
    try:
        db_session = create_connection()
        # Step 1:
        execute_sql_folder(db_session, sql_command_directory_path) 
        # Step 2 getting dvd rental staging:
        create_sql_staging_tables(db_session,SourceName.DVD_RENTAL.value)
        # Step 3 getting college staging:
        # create_sql_staging_tables(db_session,SourceName.COLLEGE)
        # Step 4 getting CSV Dataset staging:
        create_csv_staging_tables(db_session,SourceName.DATASET_MAIN.value)
        close_connection(db_session)
    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.PREHOOK_SQL_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception("Important Step Failed")