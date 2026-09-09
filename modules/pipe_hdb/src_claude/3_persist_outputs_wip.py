import os
import subprocess
import sys


########################################################################
#### copying files out from project folder into persisting location ###
########################################################################

# 1_firstbq.py now writes 3 separate chart files instead of 1 combined 1-firstbq.html
html_filenames = ['1-firstbq-overlay.html', '1-firstbq-facet.html', '1-firstbq-fixedaxis.html']

# same env-aware directory 1_firstbq.py itself writes to - output/ in-container,
# the OneDrive folder when run natively

import platform

if os.environ.get('RUNNING_IN_CONTAINER'):
    html_dir = 'output'
elif platform.system() == 'Darwin':
    html_dir = '/Users/murftech/Library/CloudStorage/OneDrive-Personal/DBMaster/annotations/reports/macroecons/pipe_hdb'
elif platform.system() == 'Windows':
    html_dir = 'C:/Users/Talesinc/OneDrive/DBMaster/annotations/reports/macroecons/pipe_hdb'


# expected_outputs = ['hive/t2/datagovhdb', 'output/1-firstbq.html']
expected_outputs = ['hive/t2/datagovhdb'] + [f'{html_dir}/{name}' for name in html_filenames]
print('If the below prints succeed, trust it because the paths are hardcoded for checking os.path.exists(path) and any bugs would have raised exception')
for path in expected_outputs:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        raise RuntimeError(f'Expected output missing or empty: {path}')
    print(f'Verified: {path} ({os.path.getsize(path):,} bytes)')


gcs_bucket = os.environ.get('GCS_BUCKET')

if gcs_bucket:

    outputs = {
        'hive/t2/datagovhdb': 'hive/t2/datagovhdb',
        **{
            f'output/{name}': f'DBMaster/annotations/reports/macroecons/pipe_hdb/{name}'
            for name in html_filenames
        },
    }  # maps each real local file path to where it should land in the bucket

    from google.cloud import storage

    bucket = storage.Client().bucket(gcs_bucket)  # authenticate and get a reference to the bucket

    for local_path, s3_path in outputs.items():
        blob = bucket.blob(s3_path)  # define the destination location inside the bucket
        blob.upload_from_filename(local_path)  # read the real local file and upload it there
        print(f'Uploaded {local_path} to gs://{gcs_bucket}/{s3_path}')  # confirm source -> destination

s3_bucket = os.environ.get('S3_BUCKET')

if s3_bucket:
    outputs = {
        'hive/t2/datagovhdb': 'hive/t2/datagovhdb',
        **{
            f'output/{name}': f'DBMaster/annotations/reports/macroecons/pipe_hdb/{name}'
            for name in html_filenames
        },
    }   # maps each real local file path to where it should land in the bucket

    import boto3
    # boto3 is AWS's official Python SDK, lets Python code talk to any AWS service 

    s3 = boto3.client('s3') # create an S3 client; credentials are auto-discovered (local ~/.aws config, or IAM role when on AWS)

    for local_path, s3_path in outputs.items():
        s3.upload_file(local_path, s3_bucket, s3_path) 
        print(f'Uploaded {local_path} to s3://{s3_bucket}/{s3_path}')

    for local_path, s3_path in outputs.items():
        
        DEFINE_FOR_OPEN_ON_URL = {'ContentType': 'text/html'} if s3_path.endswith('.html') else {}
        s3.upload_file(local_path, s3_bucket, s3_path, ExtraArgs=DEFINE_FOR_OPEN_ON_URL)
        # upload file, from path in container to destination bucket and destination location inside ethe bucket
        print(f'Uploaded {local_path} to s3://{s3_bucket}/{s3_path}')

# gcloud storage sign-url \
#     gs://murftech-macroecons/DBMaster/annotations/reports/macroecons/pipe_hdb/1-firstbq.html \
#   --duration=12h \
#   --impersonate-service-account=630862951534-compute@developer.gserviceaccount.com

# https://tinyurl.com/murftech-hdb-docker-gcp-output