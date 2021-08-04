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
# from apache_beam.dataframe.io import read_csv
from google.cloud import storage
import pandas
import json

from sys import argv
import logging

# import itertools
import datetime
import sys

def process_file():
    data = open("txt_source/020821-dtdo-rmmanual4-00000-of-00001.txt", 'r').read() # file result from ToList()
    data_splitted = data.split(", {")
    data_splitted[0] = data_splitted[0][1:]
    
    file = open("020821-dtdo-rmmanual4-00000-of-00001_edited.txt", "w")
    # print("[", end="", file=file)

    # remove duplicate
    dict_set = set()
    for i in data_splitted:
        if not i.startswith("{"):
            i = "{" + i
        dict_set.add(i.strip("]\n"))

    # convert str to dict and make list of dict
    dict_list = []
    for i in dict_set:
        dict_list.append(eval(i))
        print(i, file=file) # print dict to file if necessary

    # print("set size:", sys.getsizeof(dict_set))
    # print("list size:", sys.getsizeof(dict_list))

    return dict_list
    

    
    



    


    

if __name__ == "__main__":
    data = process_file()

    # print(data[0:5])

