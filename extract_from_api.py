import os
import json
import boto3
import requests
from datetime import datetime
import credentials
import pandas as pd
from botocore.exceptions import ClientError


def extract_json(url):
    response_api = requests.get(url)
    data = response_api.text
    parse_json = json.loads(data)

    return parse_json


def get_states(states_json):
    st = list()
    tmp_dict = dict()

    for x in states_json:
        s_c = x['state_code'].lower()
        link = f'https://api.covidtracking.com/v1/states/{s_c}/daily.json'
        j_son = extract_json(link)

        with open(f'data/{s_c}.json', 'w') as f:
            json.dump(j_son, f)

        tmp_dict = {'name': x['name'], 'state_code': x['state_code']}
        st.append(tmp_dict)
    return st


def get_dates(transactions_list):
    start = transactions_list[-1]['date']
    end = transactions_list[0]['date']

    return start, end


def gen_calendar(start_date, end_date):
    calendar = list()
    date = dict()
    dates = pd.date_range(start=start_date, end=end_date)

    for i in dates:
        date = {'date': str(i.date()), 'day': i.day, 'month': i.month, 'year': i.year}
        calendar.append(date)

    return calendar


def s3_connection(access, secret):
    # boto3.client
    # boto3.Session
    client_s3 = boto3.Session(
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name='us-east-1'
    )
    return client_s3


def write_to_s3(conn, bucket_name):
    data_folder = 'data/'

    s3 = conn.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for subdir, dirs, files in os.walk(data_folder):
        for file in files:
            path = os.path.join(subdir, file)
            with open(path, 'rb') as data:
                bucket.put_object(Key=path[len(data_folder):], Body=data)


if __name__ == '__main__':
    # extract jsons from api
    covid_states = extract_json('https://api.covidtracking.com/v2/states.json')
    covid_states = covid_states['data']

    covid_transactions = extract_json('https://api.covidtracking.com/v1/states/daily.json')

    # get state codes and generate urls for every state
    states = get_states(covid_states)

    # parsing date
    for x in range(len(covid_transactions)):
        covid_transactions[x]['date'] = datetime.strptime(str(covid_transactions[x]['date']), '%Y%m%d').date()
        covid_transactions[x]['date'] = covid_transactions[x]['date'].strftime('%Y-%m-%d')

    # calendar
    st, ed = get_dates(covid_transactions)
    calendar = gen_calendar(st, ed)

    # generate jsons into data/ to upload to s3
    
    with open('data/covid_states.json', 'w') as f:
        json.dump(states, f)

    with open('data/covid_transactions.json', 'w') as f2:
        json.dump(covid_transactions, f2)

    with open('data/calendar.json', 'w') as c:
        json.dump(calendar, c)

    # generate parquets
    df = pd.read_json('data/covid_states.json')
    df.to_parquet('data/covid_states.parquet')

    df2 = pd.read_json('data/covid_transactions.json')
    df2.to_parquet('data/covid_transactions.parquet')

    df3 = pd.read_json('data/calendar.json')
    df3.to_parquet('data/calendar.parquet')

    # write to s3 everything into data
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    bct = credentials.bucket
    s3_conn = s3_connection(access_key, secret_key)
    write_to_s3(s3_conn, bct)













