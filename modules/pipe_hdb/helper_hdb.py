import requests
import polars as pl




def fetch_hdb_data(hdb_dataset_id):

    base_url = "https://api-open.data.gov.sg/v1/public/api/datasets"

    initiate_url = f"{base_url}/{hdb_dataset_id}/initiate-download"
    print('sending')
    print(initiate_url)
    resp = requests.get(
        f"{base_url}/{hdb_dataset_id}/initiate-download",
        # headers={"Content-Type": "application/json"},
    )

    print("Fetching download URL...")
    download_url = resp.json().get("data", {}).get("url", "")
    print(download_url)

    if not download_url:
        raise RuntimeError("API did not return a download URL.")

    print("polars Reading data...")
    df = pl.read_csv(
        download_url,
        schema_overrides={"floor_area_sqm": pl.Float64, "resale_price": pl.Float64},
    )

    latest_data = df.group_by("month").len(name="number_of_sales").sort("month", descending=True)
    print('latest data count')
    latest_data.show(12)
    print(f"Done. Loaded {len(df):,} rows and {len(df.columns)} columns.")

    return df

# tester
# abc = fetch_hdb_data()
# abc.glimpse()

