import os
import json
import boto3
import requests
import credentials
from botocore.exceptions import ClientError

# extract jsons from api
def extract_json(url):
    response_api = requests.get(url)
    data = response_api.text
    parse_json = json.loads(data)

    return parse_json

# get state code from extracted json and generate urls for every state
def get_state_codes(states_json):
    states = list()
    tmp_dict = dict()

    for x in states_json:
        s_c = x['state_code'].lower()
        tmp_dict = {'name': x['name'], 'state_code': x['state_code'],
                    'current_url': f'https://api.covidtracking.com/v1/states/{s_c}/current.json'}
        states.append(tmp_dict)
    return states

# establish connection with s3
def s3_connection(access, secret):

    client_s3 = boto3.client(
        's3',
        aws_access_key_id=access,
        aws_secret_access_key=secret
    )
    return client_s3

# write every file in data/ into s3 bucket
def write_to_s3(conn, bucket_name):
    data_folder = 'data/'
    for file in os.listdir(data_folder):
        if not file.startswith('~'):
            try:
                print(f'Uploading file {file}')

                conn.upload_file(
                    os.path.join(data_folder, file),
                    bucket_name,
                    file
                )

            except ClientError as e:
                print('Wrong credentials')
                print(e)
            except Exception as e:
                print(e)


if __name__ == '__main__':
    # get s3 credentials
    access_key = credentials.access_key
    secret_key = credentials.secret_key

    # extract jsons from api
    covid_states = extract_json('https://api.covidtracking.com/v2/states.json')
    covid_states = covid_states['data']

    covid_transactions = extract_json('https://api.covidtracking.com/v1/states/current.json')

    # get state codes and generate urls for every state
    states = get_state_codes(covid_states)

    # connect to s3
    bucket = credentials.bucket
    s3_conn = s3_connection(access_key, secret_key)

    # generate jsons into data/ to upload to s3
    with open('data/covid_states.json', 'w') as f:
        json.dump(states, f)

    with open('data/covid_transactions.json', 'w') as f2:
        json.dump(covid_transactions, f2)

    # write to s3 everything into data
    write_to_s3(s3_conn, bucket)










