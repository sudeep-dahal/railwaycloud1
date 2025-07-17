import asyncio
import aiohttp
import csv
import os
import boto3
from io import StringIO
from glob import glob
import functools
from dotenv import load_dotenv


load_dotenv()

# --- Ensure Railway prints logs immediately ---
print = functools.partial(print, flush=True)

# --- Config ---
url = 'http://srv.dofe.gov.np/Services/DofeWebService.svc/GetPrePermissionByLotNo'
start = 100000
end = 327284
max_retries = 3
batch_size = 25000
file_prefix = "prepermissions"
merged_filename = "merged_pre_permissions.csv"

# --- Load AWS S3 config from environment ---
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_FILE_NAME = os.getenv('S3_FILE_NAME', merged_filename)

# --- S3 client setup ---
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# --- Upload to S3 ---
def upload_to_s3(filepath, s3_key):
    print(f"📤 Uploading {filepath} to S3 bucket {S3_BUCKET_NAME} as {s3_key}")
    try:
        with open(filepath, 'rb') as f:
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=f,
                ContentType='text/csv'
            )
        print(f"✅ Upload of {s3_key} to S3 completed.")
    except Exception as e:
        print(f"❌ Failed to upload {s3_key} to S3: {e}")

# --- Get next batch file index ---
def get_next_file_index():
    index = 1
    while os.path.exists(f"{file_prefix}{index}.csv"):
        index += 1
    return index

# --- Save each batch & upload to S3 ---
def save_batch_to_csv(data_batch, headers, batch_index):
    file_name = f"{file_prefix}{batch_index}.csv"
    print(f"💾 Saving batch {batch_index} to {file_name}")
    with open(file_name, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in data_batch:
            writer.writerow(row)

    # Upload to S3 immediately after saving
    upload_to_s3(file_name, file_name)

# --- Merge all CSVs ---
def merge_csv_files(output_filename):
    print("🔁 Merging all batch CSVs...")
    all_files = sorted(glob(f"{file_prefix}*.csv"))
    combined = {}
    headers = set()

    for file in all_files:
        print(f"📂 Reading {file}")
        with open(file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                lot_no = row.get("LotNo")
                if lot_no:
                    combined[lot_no] = row
                    headers.update(row.keys())

    with open(output_filename, 'w', newline='', encoding='utf-8') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=list(headers))
        writer.writeheader()
        for row in combined.values():
            writer.writerow(row)

    print(f"✅ Merged {len(all_files)} files into {output_filename}")
    return output_filename

# --- Retry fetch ---
async def fetch(session, lot_no):
    for attempt in range(max_retries):
        try:
            print(f"🚀 Fetching LotNo {lot_no} (Attempt {attempt + 1})")
            async with session.post(url, json={"LotNo": lot_no}, timeout=10) as response:
                data = await response.json()
                if 'd' in data and data['d']:
                    res = data['d'][0]
                    res.pop('__type', None)
                    print(f"✅ Success: LotNo {lot_no}")
                    return res
        except Exception as e:
            print(f"⚠️ Attempt {attempt + 1} failed for LotNo {lot_no}: {e}")
            await asyncio.sleep(2 ** attempt)
    print(f"❌ Failed all retries for LotNo {lot_no}")
    return None

# --- Main async runner ---
async def main():
    print("🚀 Script started")
    headers = set()
    batch_data = []
    batch_index = get_next_file_index()

    async with aiohttp.ClientSession() as session:
        for lot_no in range(start, end - 1, -1):
            print(f"📥 Queueing LotNo {lot_no}")
            res = await fetch(session, lot_no)
            if res and 'LotNo' in res:
                batch_data.append(res)
                headers.update(res.keys())

            if len(batch_data) >= batch_size:
                save_batch_to_csv(batch_data, list(headers), batch_index)
                batch_index += 1
                batch_data.clear()

            await asyncio.sleep(1)

        # Final incomplete batch
        if batch_data:
            save_batch_to_csv(batch_data, list(headers), batch_index)

    # Merge all
    merged_file = merge_csv_files(merged_filename)
    upload_to_s3(merged_file, S3_FILE_NAME)

    print("🎉 All done!")

# --- Start ---
if __name__ == '__main__':
    asyncio.run(main())
