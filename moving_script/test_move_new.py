'''
Script to move xlsx file from local folder to GCS
'''

import os
import openpyxl
from google.cloud import storage

def upload_to_bucket(file_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""

    credential = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    storage_client = storage.Client.from_service_account_json(credential)

    bucket = storage_client.get_bucket(bucket_name) 
    blob = bucket.blob(f"{file_name}")
    blob.upload_from_filename(path_to_file)

def parse_date(date):
    '''Basic date parser from m/d/yyy to yyyy_m_d'''

    date = date.split("/")
    month = date[0]
    day = date[1]
    year = date[2]

    return f'{year}_{month}_{day}'

def compare_kpi_date_parser(date):
    '''Date parser for CompareKPI file'''
    date = date.split("-")[1].strip() # at this point, the form already -> yyyy/m/d

    return parse_date(date)

def generate_filename(file_path):
    '''Method to generate filename and date, also upload'''

    wb_obj = openpyxl.load_workbook(file_path)
    sheet = wb_obj.active

    document_title = sheet["A1"].value # Cell A1 contain the title
    document_date = sheet["B2"].value # Cell B2 contain the date

    filename_dict = {
        "Daily Discounts": "DiscDailyDetails",
        "OTComparisonV1": "IQRYOTComparison",
        "Sales Take Away Fee": "IQRYSalesTakeAwayFee",
        "Tender Media": "TendersDailyDetails",
        "Store and Date Comparison Report": "CompareKPIsByDate",
        "Sales Mix Items Summary": "SalesMixItemsSummary",
    }

    # generate filename and date based on the sheet title and date
    parsed_title = filename_dict.get(document_title)
    parsed_date = parse_date(document_date) if parsed_title != "CompareKPIsByDate" else compare_kpi_date_parser(document_date)

    new_file_name = f'{parsed_title}_{parsed_date}.xlsx'

    return new_file_name



if __name__ == "__main__":
    local_path_name = "/media/arroganthooman/DATA/Fikri/UI/Magang/Script/moving_script/source_file_xlsx" # replace with your folder path
    list_file = os.listdir(local_path_name)

    for raw_file_name in list_file:
        file_path = f"{local_path_name}/{raw_file_name}"
        correct_file_name = generate_filename(file_path)

        # Upload to GCS
        bucket_name = "sb-fikri-test-move" # replace with your bucket name
        bucket_path = 'sb_data_source/adhoc' # replace with your bucket_path
        gcs_filename = f'{bucket_path}/{correct_file_name}' # Include the directory in front

        print(f"Uploading {correct_file_name}...")
        upload_to_bucket(gcs_filename, file_path, bucket_name)

        # rename local file to correct one
        print(f"Renaming '{raw_file_name}' to '{correct_file_name}'...")

        source = f'{local_path_name}/{raw_file_name}'
        dest = f'{local_path_name}/{correct_file_name}'
        os.rename(source, dest)

        print("File succesfully uploaded and renamed.\n")

    print("\nAll Upload finished.")








