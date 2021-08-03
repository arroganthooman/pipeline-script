import csv
from io import StringIO
import os

import apache_beam as beam
import argparse
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io import WriteToText
from apache_beam.io import ReadFromText
from apache_beam.coders.coders import Coder
from apache_beam.dataframe.io import read_csv
from google.cloud import storage
import pandas

from sys import argv
import logging

# import itertools
import datetime

def run():
    # df = pandas.read_csv('data_source_xlxs/data_output_District Allocation & list store July 2021.csv')
    # print(df.to_string())
    with open("data_source_xlxs/data_output_District Allocation & list store July 2021.csv") as data:
        file_result = open("result.txt", 'w')
        reader = csv.DictReader(data)
        # for row in reader:
        #     print(row, file=file_result)
        list_of_dict = list(reader)[0]
        
        file_result.close()

        original_key = [
        'Site Code', 'Store Name', 'City', 'Type', 'Region No.', 'Region Name', 'District No.', 
        'District Name', 'Opening Date', 'Address', 'Telephone', 'Email', 'ID SM', 'STORE MANAGER', 
        'SM HP Number', 'Opertaional Hours Store for Dine in', 
        'Opertaional Hours Store for take away', 'Community Store', 'DriveThru', 'Reserve', 'Nitro', 
        'Coffee Forward', 'Delivery Store', 'Ice Cream', 'Fizzio', 'MPOS', 'Masterclass', 'Digital Menu Board', 
        '2 Mastrena EOC', 'Cashless', 'ESB', 'Shopee Food'
        ]
    
        modified_key = [
            "site_code", "store_name", "city", "type", "region_no", "region_name", "district_no",
            "district_name", "opening_date", "address","telephone","email","id_sm","store_manager","sm_hp_number",
            "operational_hours_store_for_dine_in", "operational_hours_store_for_take_away",
            "community_store","drive_thru","reserve","nitro","coffee_forward","delivery_store",
            "ice_cream","fizzio","mpos","masterclass","digital_menu_board",
            "two_mastrena_eoc","cashless","esb","shopee_food"
        ]

        list_of_dict.pop('No.')

        for i in range(len(original_key)):
            list_of_dict[modified_key[i]] = list_of_dict.pop(original_key[i])

        for i in list_of_dict.items():
            print(i)

    # data = [
    #     "site_code",
    #     "store_name",
    #     "city",
    #     "type",
    #     "region_no",
    #     "region_name",
    #     "district_no",
    #     "district_name",
    #     "address",
    #     "telephone",
    #     "email",
    #     "id_sm",
    #     "store_manager",
    #     "sm_hp_number",
    #     "operational_hours_store_for_dine_in",
    #     "operational_hours_store_for_take_away",
    #     "community_store",
    #     "drive_thru",
    #     "reserve",
    #     "nitro",
    #     "coffee_forward",
    #     "delivery_store",
    #     "ice_cream",
    #     "fizzio",
    #     "mpos",
    #     "fizzio",
    #     "masterclass",
    #     "digital_menu_board",
    #     "two_mastrena_eoc",
    #     "cashless",
    #     "esb",
    #     "shopee_food"
    # ]

    



    


    

if __name__ == "__main__":
    storage_client = storage.Client.from_service_account_json(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    bucket = storage_client.bucket("sb_xlsx_data_source")
    blobs = list(bucket.list_blobs(prefix='data_output/'))
    # print(blobs[3])
    blob = blobs[3].download_as_string()
    print(blob)
    # blob = StringIO(blob)  #tranform bytes to string here

    # reader = csv.DictReader(blob)  #then use csv library to read the content
    # print(list(blobs))


# [
#     "site_code",
#     "store_name",
#     "city",
#     "type",
#     "region_no",
#     "region_name",
#     "district_no",
#     "district_name",
#     "address",
#     "telephone",
#     "email",
#     "id_sm",
#     "store_manager",
#     "sm_hp_number",
#     "operational_hours_store_for_dine_in",
#     "operational_hours_store_for_take_away",
#     "community_store",
#     "drive_thru",
#     "reserve",
#     "nitro",
#     "coffee_forward",
#     "delivery_store",
#     "ice_cream",
#     "fizzio",
#     "mpos",
#     "fizzio",
#     "masterclass",
#     "digital_menu_board",
#     "two_mastrena_eoc",
#     "cashless",
#     "esb",
#     "shopee_food"
# ]


