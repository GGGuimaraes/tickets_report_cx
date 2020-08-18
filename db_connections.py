import json
import psycopg2
import s3


def connect(credentials_name):
    credentials = json.loads(s3.extract_file_as_string(
        'credentials/' + credentials_name))
    return psycopg2.connect(host=credentials['host'],
                            port=int(credentials['port']),
                            dbname=credentials['dbname'],
                            user=credentials['user'],
                            password=credentials['passwd'])

def extract_file_as_string(path):
    s3_object = S3.Object('blu-etl', path)
    return s3_object.get()['Body'].read().decode('utf-8')



def connect_to_send(credentials_name):
    credentials = json.loads(extract_file_as_string(
        'credentials/' + credentials_name))
    
    return create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'
                        .format(credentials['user'], 
                                credentials['passwd'],
                                credentials['host'],
                                credentials['port'],
                                credentials['dbname']))