import psycopg2
from sqlalchemy import create_engine
import dotenv, os
from dotenv import dotenv_values
import pandas as pd
import psycopg2, boto3

# Create a boto3 s3 client for bucket operations
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def get_redshift_conn():
    dotenv.load_dotenv(r"C:\Users\NGSL0161\Desktop\Data Engineering class\projects\environmental variables\.env")
    IAM_ROLE = os.getenv('REDSHIFT_IAM_ROLE')
    USER = os.getenv('REDSHIFT_USER')
    PASSWORD = os.getenv('REDSHIFT_PASSWORD')
    PORT = os.getenv('REDSHIFT_PORT')
    HOST = os.getenv('REDSHIFT_PORT')
    DEV = os.getenv('REDSHIFT_DATABASE_NAME')
    return IAM_ROLE, USER, PASSWORD, PORT, HOST, DEV




def generate_schema(data, table_name = 'log_data'):
    create_table_statement = f'CREATE TABLE IF NOT EXISTS {table_name}(\n'
    column_type_query = ''
    
    types_checker = {
        'INT':pd.api.types.is_integer_dtype,
        'VARCHAR':pd.api.types.is_string_dtype,
        'FLOAT':pd.api.types.is_float_dtype,
        'TIMESTAMP':pd.api.types.is_datetime64_any_dtype,
        'OBJECT':pd.api.types.is_dict_like,
        'ARRAY':pd.api.types.is_list_like,
    }
    for column in data: # Iterate through all the columns in the dataframe
        last_column = list(data.columns)[-1] # Get the name of the last column
        for type_ in types_checker: 
            mapped = False
            if types_checker[type_](data[column]): # Check each column against data types in the type_checker dictionary
                mapped = True # A variable to store True of False if there's type is found. Will be used to raise an exception if type not found
                if column != last_column: # Check if the column we're checking its type is the last comlumn
                    column_type_query += f'{column} {type_},\n' # 
                else:
                    column_type_query += f'{column} {type_}\n'
                break
        if not mapped:
            raise ('Type not found')
    column_type_query += ');'
    output_query = create_table_statement + column_type_query
    return output_query



def execute_sql(sql_query, conn):
    conn = get_redshift_conn()
    cur = conn.cursor() # Creating a cursor object for executing SQL query
    cur.execute(sql_query)
    conn.commit()
    cur.close() # Close cursor
    conn.close() # Close connection

def move_files_to_processed_folder(bucket_name, raw_data_folder, processed_data_folder):
    file_paths = list_files_in_folder(bucket_name, raw_data_folder)
    for file_path in file_paths:
        file_name = file_path.split('/')[-1]
        copy_source = {'Bucket': bucket_name, 'Key': file_path}
        # Copy files to processed folder
        s3_resource.meta.client.copy(copy_source, bucket_name, processed_data_folder + '/' + file_name)
        s3_resource.Object(bucket_name, file_path).delete()
    print("Files successfully moved to 'processed_data' folder in S3")

def list_files_in_folder(bucket_name, folder):
    bucket_list = s3_client.list_objects(Bucket = bucket_name, Prefix = folder) # List the objects in the bucket
    bucket_content_list = bucket_list.get('Contents')
    files_list = [file.get('Key') for file in bucket_content_list][1:]
    return files_list