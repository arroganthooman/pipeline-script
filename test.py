import csv

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

from sys import argv
import logging

# import itertools
import datetime

def run():
    # with open("data_source_xlxs/data_output_District Allocation & list store July 2021.csv") as data:
    #     file_result = open("result.txt", 'w')
    #     reader = csv.DictReader(data)
    #     for row in reader
    #         print(row, file=file_result)

    #     file_result.close()

    data = [
        "site_code",
        "store_name",
        "city",
        "type",
        "region_no",
        "region_name",
        "district_no",
        "district_name",
        "address",
        "telephone",
        "email",
        "id_sm",
        "store_manager",
        "sm_hp_number",
        "operational_hours_store_for_dine_in",
        "operational_hours_store_for_take_away",
        "community_store",
        "drive_thru",
        "reserve",
        "nitro",
        "coffee_forward",
        "delivery_store",
        "ice_cream",
        "fizzio",
        "mpos",
        "fizzio",
        "masterclass",
        "digital_menu_board",
        "two_mastrena_eoc",
        "cashless",
        "esb",
        "shopee_food"
    ]

    for text in data:
        print(f"data['{text}'] = str(data['{text}']) if '{text}' in data else None")

    


    

if __name__ == "__main__":
    run()
    print("test")


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


