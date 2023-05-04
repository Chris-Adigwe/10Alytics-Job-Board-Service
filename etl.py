import pandas as pd
import requests,os,dotenv
from datetime import datetime, timedelta
import io
from io import StringIO
from datetime import datetime
import boto3
from dotenv import dotenv_values
from utils import get_redshift_conn,execute_sql, list_files_in_folder,move_files_to_processed_folder

dotenv_values()

s3_client = boto3.client('s3')

bucket_name = 'chris-raw-jobs-data'
transformed_bucket_name = 'chris-transformed-jobs-data'
path = 'raw_jobs_data'
transformed_path_name = 'chris-transformed-jobs'

#created an extract function from the api
def extract_from_API_(url,countries,jobs):
    all_data = pd.DataFrame()
    for i in countries:
        for j in jobs:
            querystring = {"query":f"{i}, {j}","page":"1","num_pages":"1"}
            headers = {
                    "X-RapidAPI-Key": "b4ae60111cmshba38a6d6440bd93p1eb62ejsn40f5b1666a74",
                    "X-RapidAPI-Host": "jsearch.p.rapidapi.com"}
            response = requests.get(url, headers=headers, params=querystring)
            response = response.json()
            data = response.get('data')
            data = pd.DataFrame(data)
            all_data = pd.concat([all_data, data])

    return all_data


def load_to_s3():
    countries = ['USA', 'UK', 'Canada']
    jobs = ['Data engineer', 'Data Analyst']
    url = "https://jsearch.p.rapidapi.com/search"

    data = extract_from_API_(url,countries,jobs)
    
    file_name = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
    csv_buffer = StringIO()
    data.reset_index(drop=True, inplace=True)
    data.to_json(csv_buffer,orient='columns')
    csv_str = csv_buffer.getvalue()


    s3_client.put_object(Bucket=bucket_name, Key = f'{path}/{file_name}', Body=csv_str)

    print("file loaded successfully to the s3 bucket")
    

def read_transform_files_from_s3():
    columns=['employer_website', 'job_id', 'job_employment_type', 'job_title','job_apply_link', 'job_description', 'job_city', 'job_country','job_posted_at_datetime_utc', 'employer_company_type']

    objects_list = s3_client.list_objects(Bucket=bucket_name, Prefix=path)
    file = objects_list.get('Contents')[1]
    Key = file.get('Key')
    obj = s3_client.get_object(Bucket = bucket_name, Key=Key)
    data = pd.read_json(io.BytesIO(obj['Body'].read()))
    data = data[columns]
    data['job_posted_at_datetime_utc'] = data['job_posted_at_datetime_utc'].map(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ').date())
    today = datetime.now().date()
    start_of_week = today - timedelta(days=today.weekday())
    data = data[data['job_posted_at_datetime_utc'] >= start_of_week]
    

    file_name = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
    csv_buffer = StringIO()
    data.reset_index(drop=True, inplace=True)
    data.to_csv(csv_buffer,index=False)
    csv_str = csv_buffer.getvalue()

    s3_client.put_object(Bucket=transformed_bucket_name, Key = f'{transformed_path_name}/{file_name}', Body=csv_str)

    print("tranfomed file loaded successfully")
    return file_name

    
    

def load_to_redshift(table_name,file_name):
    s3_path = f's3://{transformed_bucket_name}/{transformed_path_name}/{file_name}' # Replace this with your file path (bucket name, folder & file name)
    dotenv.load_dotenv(r"C:\Users\NGSL0161\Desktop\Data Engineering class\projects\environmental variables\.env")
    iam_role = os.getenv('REDSHIFT_IAM_ROLE')
    conn = get_redshift_conn()
    # A copy query to copy csv files from S3 bucket to Redshift.
    copy_query = f"""
    copy {table_name}
    from '{s3_path}'
    IAM_ROLE '{iam_role}'
    csv
    IGNOREHEADER 1;
    """
    execute_sql(copy_query, conn)
    print('Data successfully loaded to Redshift')


