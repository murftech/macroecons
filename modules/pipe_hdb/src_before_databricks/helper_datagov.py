from pathlib import Path
import time
import requests


def fetch_datagov_csv(dataset_id, dest_path, max_attempts=10, poll_interval=5):
    '''
    Download a csv from data.gov.sg to dest_path based on publicized API shape.
    '''

    dest_path = Path(dest_path)

    base_url = "https://api-open.data.gov.sg/v1/public/api/datasets"

    initiate_url = f"{base_url}/{dataset_id}/initiate-download"
    print('sending')
    print(initiate_url)
    requests.get(
        initiate_url,
    )

    download_url = ""
    for i in range(1, max_attempts + 1):
        resp = requests.get(f"{base_url}/{dataset_id}/poll-download")

        print("Trying Fetching download URL...")
        try:
            download_url = resp.json().get("data", {}).get("url", "")
        except (ValueError, AttributeError):
            download_url = ""

        if download_url:
            break
        time.sleep(poll_interval)

    if not download_url:
        raise RuntimeError(f"API did not return a download URL after {max_attempts} polling attempts.")

    # 3. pull csv and write bytes to dest_path
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(download_url)
    dest_path.write_bytes(r.content)

    print(f"landed file: {dest_path} ({dest_path.stat().st_size / 1_000_000:.1f} MB)")


# unit_test
# Random Data
# https://data.gov.sg/datasets?topics=health&resultId=d_d4386b3130f3e5f7ccbbfc785602a606
# d_d4386b3130f3e5f7ccbbfc785602a606
# fetch_datagov_csv(dataset_id = 'd_d4386b3130f3e5f7ccbbfc785602a606', dest_path='hive/tmpgov/anydata.csv')

