from datetime import date
from google.cloud import storage
import os
import requests

def upload_to_bucket(file_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""

    credential = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    storage_client = storage.Client.from_service_account_json(credential)

    bucket = storage_client.get_bucket(bucket_name) 
    blob = bucket.blob(f"{file_name}")
    blob.upload_from_filename(path_to_file)


if __name__ == "__main__":
    # replace pathname to your local folder pathname
    pathname = "/media/arroganthooman/DATA/Fikri/UI/Magang/Script/moving_script/source_file_txt"
    list_file = os.listdir(pathname)

    for file_name in list_file:
        file_path = f'{pathname}/{file_name}'

        bucket_name = "sb-fikri-test-move" # replace with your bucket-name
        bucket_path = "sb_data_source/adhoc/txt_file" # replace with with your bucket/folder path
        gcs_filename = f'{bucket_path}/{file_name[21:]}'

        print(f"Uploading {gcs_filename}...")
        upload_to_bucket(gcs_filename, file_path, bucket_name)

    
    print("File uploaded.")
