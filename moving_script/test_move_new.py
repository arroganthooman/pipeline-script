'''
Script to move xlsx file from local to GCS
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

def generate_filename_and_upload(file_path):
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

    # generate filename and date based on the sheet
    parsed_filename = filename_dict.get(document_title)
    parsed_date = parse_date(document_date) if parsed_filename != "CompareKPIsByDate" else compare_kpi_date_parser(document_date)

    # File upload configuration
    file_name_to_upload = f'data_source/adhoc/{parsed_filename}_{parsed_date}.xlsx'
    bucket_name = "sb-fikri-test-move" #replace with your bucket name

    print(f"Uploading {file_name_to_upload}....")
    upload_to_bucket(file_name_to_upload, file_path, bucket_name)

if __name__ == "__main__":
    path_name = "/media/arroganthooman/DATA/Fikri/UI/Magang/Script/moving_script/source_file_xlsx" # replace with your folder path
    list_file = os.listdir(path_name)

    for file in list_file:
        file_path = f"{path_name}/{file}"
        generate_filename_and_upload(file_path)

    print("Upload finished.")








