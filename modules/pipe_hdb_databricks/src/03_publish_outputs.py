"""Databricks version of the second half of modules/pipe_hdb/run_pipeline.py
(the "verify outputs exist, then optionally push to GCS/S3" section).

What changed and why:
  - The verify step now checks the Unity Catalog Volume path instead of
    'hive/t2/datagovhdb' + 'output/*.html' - same idea (fail loudly if a file is
    missing or empty), same os.path.exists/os.path.getsize calls, since Volumes
    are POSIX paths and normal Python file I/O just works on them.
  - The GCS/S3 upload is still here and still opt-in via the same env-var pattern
    (now bundle variables --gcs-bucket / --s3-bucket instead of GCS_BUCKET/S3_BUCKET
    env vars) - useful if you still want a copy landing in the same bucket your
    OneDrive-synced reports used to read from. Leave both blank to skip entirely;
    the Volume copy from 02_report_plotly.py is enough on its own for most purposes.
  - Credentials: don't hardcode a service account key or AWS key here. Store them
    in a Databricks secret scope (`databricks secrets create-scope hdb-pipeline`,
    then `databricks secrets put-secret ...`) and read them via os.environ if you
    wire the job task's `spark_env_vars` to `{{secrets/hdb-pipeline/...}}` in
    resources/hdb_pipeline_job.yml. Left as a TODO below rather than guessed at,
    since it depends on which cloud your Databricks workspace itself runs on.
"""

import argparse
import os
from datetime import date


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', required=True)
    parser.add_argument('--schema', required=True)
    parser.add_argument('--volume', required=True)
    parser.add_argument('--gcs-bucket', default='')
    parser.add_argument('--s3-bucket', default='')
    args = parser.parse_args()

    volume_root = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}"
    run_dir = f"{volume_root}/{date.today().isoformat()}"

    html_filenames = ['1-firstbq-overlay.html', '1-firstbq-facet.html', '1-firstbq-fixedaxis.html']
    expected_outputs = [f'{run_dir}/{name}' for name in html_filenames]

    print('Verifying today\'s outputs exist in the Volume:')
    for path in expected_outputs:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            raise RuntimeError(f'Expected output missing or empty: {path}')
        print(f'  Verified: {path} ({os.path.getsize(path):,} bytes)')

    if args.gcs_bucket:
        # TODO: set spark_env_vars in resources/hdb_pipeline_job.yml to inject GCP
        # credentials from a Databricks secret scope before relying on this branch.
        from google.cloud import storage

        bucket = storage.Client().bucket(args.gcs_bucket)
        for name in html_filenames:
            local_path = f'{run_dir}/{name}'
            gcs_path = f'DBMaster/annotations/reports/macroecons/pipe_hdb/{name}'
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            print(f'Uploaded {local_path} to gs://{args.gcs_bucket}/{gcs_path}')

    if args.s3_bucket:
        # TODO: same credentials note as above, but for an AWS secret.
        import boto3

        s3 = boto3.client('s3')
        for name in html_filenames:
            local_path = f'{run_dir}/{name}'
            s3_path = f'DBMaster/annotations/reports/macroecons/pipe_hdb/{name}'
            extra_args = {'ContentType': 'text/html'} if s3_path.endswith('.html') else {}
            s3.upload_file(local_path, args.s3_bucket, s3_path, ExtraArgs=extra_args)
            print(f'Uploaded {local_path} to s3://{args.s3_bucket}/{s3_path}')

    if not args.gcs_bucket and not args.s3_bucket:
        print('No gcs_bucket/s3_bucket set - reports live only in the Volume, which is fine.')


if __name__ == '__main__':
    main()
