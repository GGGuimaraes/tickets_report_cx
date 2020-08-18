import boto3
import sys
import pandas as pd

if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO



S3 = boto3.resource('s3')


def extract_file_as_string(path):
    s3_object = S3.Object('blu-etl', path)
    return s3_object.get()['Body'].read().decode('utf-8')


def list_files_in_path(path):
    bucket = S3.Bucket('blu-etl')
    return [obj.key.rsplit('/')[-1]
            for obj in bucket.objects.filter(Prefix=path)]


def insert_filles_in_path(path, arq_csv):
    s3 = boto3.client('s3')
    #bucket = s3.Bucket('ted_fraud_detection_lake')
    s3.upload_file(Bucket = 'blu-etl', Key='client_score/'+ path, Filename = arq_csv)
    

def extract_csv_into_pandas(path, sep = ';', date_to_be_parsed= False):
    
    try:
        data_as_string=extract_file_as_string(path)
    except:
        print ("Error while Extracting")
        
    text_data = StringIO(data_as_string)
    return pd.read_csv(text_data, sep=sep, parse_dates = date_to_be_parsed)