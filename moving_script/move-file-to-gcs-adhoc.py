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
    # Get the list of all file in source_file_xlxs folder
    pathname = "/media/arroganthooman/DATA/Fikri/UI/Magang/Script/moving_script/source_file_xlsx" # replace with your folder pathname
    list_file = os.listdir(pathname)

    bucket_name = "sb-fikri-test-move" # rename with your bucket

    # Configuring today's date
    date_today = date.today()
    month_today = date_today.month
    year_today = date_today.year
    day_today = date_today.day

    # testing purpose
    # month_today = 6
    # year_today = 2021
    # day_today = 2


    # upload each file to bucket
    print("Uploading...")
    for file_name in list_file:
        # Get the file date
        file_name = file_name.rstrip(".xlsx")
        file_name_splitted = file_name.split("_")
        file_day = int(file_name_splitted[-1])
        file_month = int(file_name_splitted[-2])
        file_year = int(file_name_splitted[-3])

        # Compare file's date and today's date
        toUpload = (year_today, month_today, day_today) == (file_year, file_month, file_day)
        
        if toUpload:
            path_to_file = f'{pathname}/{file_name}.xlsx'
            upload_to_bucket(file_name, path_to_file, bucket_name)

    print("File Uploaded.")

    
    