# --- libaries ---

import configparser
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pytz import utc
import os, sys
import requests
import time

from google.cloud import bigquery

# --- functions ---

def print_message(section, message = None):

    dt_now = datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S')

    if message is None:

        return print(f'{dt_now}: [{section}] \n')
    
    else:

        return print(f'{dt_now}: [{section}] - {message} \n')


def normalise_response(response):

    n_rows = len(response)
    response_norm = []

    # iterating over each row in response
    for i in range(n_rows):

        # normalising nested row dict
        row_df = pd.json_normalize(response[i], sep='_')

        row_dict = row_df.to_dict(orient='records')

        response_norm += row_dict

    response_norm_df = pd.DataFrame(response_norm)

    return response_norm_df

def process_response(response, schema):

    response_norm_df = normalise_response(response)

    response_processed_df = response_norm_df.copy()

    response_processed_df['updated_at'] = datetime.now(tz=utc).replace(microsecond=0)

    dt_cols = [col['column_name'] for col in schema if col['data_type'] == 'DATETIME']

    for dt_col in dt_cols:
        response_processed_df[dt_col] = pd.to_datetime(response_processed_df[dt_col], utc=True)

    str_cols = [col['column_name'] for col in schema if col['data_type'] == 'STRING']
    for str_col in str_cols:
        response_processed_df[str_col] = response_processed_df[str_col].fillna("")
        response_processed_df[str_col] = response_processed_df[str_col].astype(str)

    int_cols = [col['column_name'] for col in schema if col['data_type'] == 'INTEGER']
    for int_col in int_cols:
        response_processed_df[int_col] = response_processed_df[int_col].fillna(-1)
        response_processed_df[int_col] = response_processed_df[int_col].astype(int)

    return response_processed_df

def get_row_count(table_id):

    query = f"""
    SELECT
        COUNT(*) AS total_rows
    FROM
        `{table_id}`
    """

    df = bq_client.query(query).result().to_dataframe()

    n_rows = int(df.loc[0, 'total_rows'])

    return n_rows

def load_to_bq(df, table_id, write_disposition, schema):

    job_config = bigquery.LoadJobConfig(

        schema=[bigquery.SchemaField(col['column_name'], col['data_type']) for col in schema],

        write_disposition=write_disposition,
    )

    load_job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)  

    load_job.result()  

    n_rows = load_job.output_rows

    return print(f"Loaded {n_rows} rows to {table_id} \n")

def get_latest_date(table_id):

    query = f"""
        SELECT 
            MAX(fixture_date) AS latest_date 
        FROM 
            `{table_id}`
        """

    query_job = bq_client.query(query) 

    df = query_job.result().to_dataframe()

    latest_date = df.loc[0, 'latest_date'].date()

    return latest_date

def delete_latest_date(table_id, latest_date_str):

    query = f"""
        DELETE
        FROM
            `{table_id}`
        WHERE
            CAST(fixture_date AS DATE) = CAST('{latest_date_str}' AS DATE)
        """

    delete_job = bq_client.query(query)
    
    delete_job.result()  

    n_rows = delete_job.num_dml_affected_rows

    return print(f"deleted {n_rows} rows from {table_id}, where date = {latest_date_str} \n")

# --- classes ---

class APIFootball():

    def __init__(self):

        self.headers = {k:v for k,v in config['API_FOOTBALL'].items()}

    def get(self, url, params = {}, timeout = 5, sleep = 5, max_retries = 2, backoff = 2):

        i = 0

        while i <= max_retries:

            t_1 = time.time()

            r = requests.request("GET", url, headers=self.headers, params=params, timeout = timeout)

            t_2 = time.time()
            t_12 = t_2 - t_1

            print(f'{r.url} \n')

            print(f'API request elapsed in {round(t_12, 2)}s, with HTTP status: {r.status_code} {r.reason} \n')

            if r.status_code == requests.codes.ok:

                response = r.json().get('response')  

                return response
            
            elif r.status_code in [408]:

                print('Retrying in {sleep}s \n')

                time.sleep(sleep)

                sleep *= backoff
                i += 1

            else:

                return sys.exit()

        print('Retry limit exceeded \n')

        return sys.exit()

# --- variables, clients, objects ---

config = configparser.ConfigParser()
config.read('credentials.ini')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['GCP']['GOOGLE_APPLICATION_CREDENTIALS']

bq_client = bigquery.Client()

with open('results.json', 'r') as f:
    api_endpoints = json.load(f)

api_obj = APIFootball()

### --- main ---

print('--- SCRIPT START --- \n')

end_date = datetime.now()
end_date_str = datetime.strftime(end_date, '%Y-%m-%d')

for api_endpoint in api_endpoints:

    endpoint_name = api_endpoint['endpoint_name']

    table_id = api_endpoint['table_id']
    schema = api_endpoint['schema']
    write_disposition = api_endpoint['write_disposition']

    url = api_endpoint['url']
    params = api_endpoint['params']

    from_date = get_latest_date(table_id)
    from_date_str = datetime.strftime(from_date, '%Y-%m-%d')

    print_message(endpoint_name, '0: Deleting data')

    delete_latest_date(table_id, from_date_str)

    date_params = {
        "from": from_date_str,
        "to": end_date_str
    }

    params = {**params, **date_params}

    print_message(endpoint_name, '1: Getting data')

    response = api_obj.get(url=url, params=params)

    n_rows = len(response)

    if n_rows > 0:

        print_message(endpoint_name, '2: Processing data')

        df = process_response(response, schema = schema)

        print_message(endpoint_name, '3: Loading data to BQ')

        load_to_bq(df, table_id, write_disposition, schema)
    
    else:

        print("No rows returned, skipping to next endpoint \n")

        continue

print('--- SCRIPT END ---')

