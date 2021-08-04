from google.cloud import storage
import os

def upload_to_bucket(file_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""

    credential = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    storage_client = storage.Client.from_service_account_json(credential)

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(path_to_file)


if __name__ == "__main__":
    # Get the list of all file in source_file_xlxs folder
    pathname = "/media/arroganthooman/DATA/Fikri/UI/Magang/Script/moving_script/source_file_xlsx" # replace with your folder pathname
    list_file = os.listdir(pathname)

    bucket_name = "sb-fikri-test-move" # rename with your bucket

    # upload each file to bucket
    print("Uploading...")
    for file_name in list_file:
        path_to_file = f'{pathname}/{file_name}'
        upload_to_bucket(file_name, path_to_file, bucket_name)

    print("File Uploaded.")
    