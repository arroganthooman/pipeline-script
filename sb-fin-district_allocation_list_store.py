from __future__ import absolute_import

import apache_beam as beam
import argparse
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io import WriteToText
from apache_beam.io import ReadFromText
from apache_beam.coders.coders import Coder
from sys import argv
import logging
import string
# import itertools
import datetime


# ====================CONVERT TYPE FUNCTIONS=============

class CustomCoder(Coder):

    def encode(self, value):
        return value.encode("utf-8", "ignore")

    def decode(self, value):
        return value.decode("utf-8", "ignore")

    def is_deterministic(self):
        return True

def convert_types_DistrictAllocationAndListStore(data):
    """Converts string values to their appropriate type."""

    date_format = '%Y-%m-%dT00:00:00+00:00'


    # data["site_code"] =  data["site_code"] if "site_code" in data else None
    # data["store_name"] = data["store_name"] if "store_name" in data else None
    # data["city"]=  data["city"] if "city" in data else None
    # data["type"] = data["type"] if "type" in data else None
    # data["region_no"] = data["region_no"] if "region_no" in data else None
    # data["region_name"] =  data["region_name"] if "region_name" in data else None
    # data["district_no"] = data["district_no"] if "district_no" in data else None
    # data["district_name"] = data["district_name"] if "district_name" in data else None
    # data["address"] = data["address"] if "address" in data else None
    # data["telephone"] = data["telephone"] if "telephone" in data else None
    # data["email"] = x[12]
    # data["id_sm"] = x[13]
    # data["store_manager"] = x[14]
    # data["sm_hp_number"] = x[15]
    # data["operational_hours_store_for_dine_in"] = x[16]
    # data["operational_hours_store_for_take_away"] = x[17]
    # data["community_store"] = x[18]
    # data["drive_thru"] = x[19]
    # data["reserve"] = x[20]
    # data["nitro"] = x[21]
    # data["coffee_forward"] = x[22]
    # data["delivery_store"] = x[23]
    # data["ice_cream"] = x[24]
    # data["fizzio"] = x[25]
    # data["mpos"] = x[26]
    # data["fizzio"] = x[27]
    # data["masterclass"] = x[28]
    # data["digital_menu_board"] = x[29]
    # data["two_mastrena_eoc"] = x[30]
    # data["cashless"] = x[31]
    # data["esb"] = x[31]
    # data["shopee_food"] = x[32]

    data['site_code'] = str(data['site_code']) if 'site_code' in data else None
    data['store_name'] = str(data['store_name']) if 'store_name' in data else None
    data['city'] = str(data['city']) if 'city' in data else None
    data['type'] = str(data['type']) if 'type' in data else None
    data['region_no'] = str(data['region_no']) if 'region_no' in data else None
    data['region_name'] = str(data['region_name']) if 'region_name' in data else None
    data['district_no'] = str(data['district_no']) if 'district_no' in data else None
    data['district_name'] = str(data['district_name']) if 'district_name' in data else None
    data['address'] = str(data['address']) if 'address' in data else None
    data['telephone'] = str(data['telephone']) if 'telephone' in data else None
    data['email'] = str(data['email']) if 'email' in data else None
    
    data['id_sm'] = data['id_sm'] if "id_sm" in data else None
    
    data['store_manager'] = str(data['store_manager']) if 'store_manager' in data else None
    
    # masih aneh datanya
    data['sm_hp_number'] = str(data['sm_hp_number']) if 'sm_hp_number' in data else None 
    
    data['operational_hours_store_for_dine_in'] = str(data['operational_hours_store_for_dine_in']) if 'operational_hours_store_for_dine_in' in data else None
    data['operational_hours_store_for_take_away'] = str(data['operational_hours_store_for_take_away']) if 'operational_hours_store_for_take_away' in data else None
    data['community_store'] = str(data['community_store']) if 'community_store' in data else None
    data['drive_thru'] = str(data['drive_thru']) if 'drive_thru' in data else None
    data['reserve'] = str(data['reserve']) if 'reserve' in data else None
    data['nitro'] = str(data['nitro']) if 'nitro' in data else None
    data['coffee_forward'] = str(data['coffee_forward']) if 'coffee_forward' in data else None
    data['delivery_store'] = str(data['delivery_store']) if 'delivery_store' in data else None
    data['ice_cream'] = str(data['ice_cream']) if 'ice_cream' in data else None
    data['fizzio'] = str(data['fizzio']) if 'fizzio' in data else None
    data['mpos'] = str(data['mpos']) if 'mpos' in data else None
    data['masterclass'] = str(data['masterclass']) if 'masterclass' in data else None
    data['digital_menu_board'] = str(data['digital_menu_board']) if 'digital_menu_board' in data else None
    data['two_mastrena_eoc'] = str(data['two_mastrena_eoc']) if 'two_mastrena_eoc' in data else None
    data['cashless'] = str(data['cashless']) if 'cashless' in data else None
    data['esb'] = str(data['esb']) if 'esb' in data else None
    data['shopee_food'] = str(data['shopee_food']) if 'shopee_food' in data else None

    # date = datetime.datetime.strptime(data['opening_date'], date_format)
    # data['opening_date'] = str(date.date())
    # data['opening_date_year'] = str(date.year)
    # data['opening_date_month'] = str(date.month)
    # data['opening_date_day'] = str(date.day)
    # data['opening_date_dayname'] = str(date.strftime("%A"))
    # data['opening_date_weeks'] = str(date.strftime("%W"))

    return data

schema_tenders_master = (
    'discount_type:STRING,\
    percent:FLOAT,\
    total:FLOAT,\
    percent_of_total:FLOAT,\
    count:INTEGER,\
    average:FLOAT,\
    date:DATE,\
    date_year:STRING,\
    date_month:STRING,\
    date_day:STRING,\
    date_dayname:STRING,\
    date_weeks:STRING'
    )

project_id = 'wired-glider-289003'  # replace with your project ID
dataset_id = 'starbuck_data_samples'  # replace with your dataset ID
table_id_tender = 'SB_FIN_DistrictAllocationAndListStore'

    # parameters
    # project_id='wired-glider-289003'
    # job_name='lingga-test-sb'
    # temp_location='gs://$PROJECT/temp'
    # max_num_workers=3
    # worker_region='asia-southeast1'

def run(argv=None):

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)
    # options=PipelineOptions(
        # runner='DataflowRunner',
    #     project=project_id,
    #     job_name=job_name,
    #     temp_location=temp_location,
    #     region=worker_region,
        # autoscaling_algorithm='THROUGHPUT_BASED',
        # max_num_workers=max_num_workers
    # )

    # p = beam.Pipeline(options)
    # p = beam.Pipeline(options=PipelineOptions())

    # pipeline_options = PipelineOptions(pipeline_args)
    # pipeline_options = PipelineOptions(runner)
    # pipeline_options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=PipelineOptions())
# with beam.Pipeline(options=PipelineOptions) as p:
    DistrictAllocationAndListStore_data = (p 
                        # | 'ReadData DistrictAllocationAndListStore data' >> beam.io.ReadFromText('gs://sb_xlsx_data_source/data_output/data_output_District Allocation & list store July 2021.csv', skip_header_lines =1)
                        | 'ReadData DistrictAllocationAndListStore data' >> beam.io.ReadFromText('data_source_xlxs/data_output_District Allocation & list store July 2021.csv', skip_header_lines =1)
                        | 'SplitData DistrictAllocationAndListStore' >> beam.Map(lambda x: x.split(',')) #delimiter comma (,)
                        | 'FormatToDict DistrictAllocationAndListStore' >> beam.Map(lambda x: {
                            "site_code": x[1].strip(),
                            "store_name": x[2].strip(),
                            "city": x[3].strip(),
                            "type": x[4].strip(),
                            "region_no": x[5].strip(),
                            "region_name": x[6].strip(),
                            "district_no": x[7].strip(),
                            "district_name": x[8].strip(),
                            "address": x[10].strip(),
                            "telephone": x[11].strip(),
                            "email": x[12].strip(),
                            "id_sm": x[13].strip(),
                            "store_manager": x[14].strip(),
                            "sm_hp_number": x[15].strip(),
                            "operational_hours_store_for_dine_in": x[16].strip(),
                            "operational_hours_store_for_take_away": x[17].strip(),
                            "community_store": x[18].strip(),
                            "drive_thru": x[19].strip(),
                            "reserve": x[20].strip(),
                            "nitro": x[21].strip(),
                            "coffee_forward": x[22].strip(),
                            "delivery_store": x[23].strip(),
                            "ice_cream": x[24].strip(),
                            "fizzio": x[25].strip(),
                            "mpos": x[26].strip(),
                            "fizzio": x[27].strip(),
                            "masterclass": x[28].strip(),
                            "digital_menu_board": x[29].strip(),
                            "two_mastrena_eoc": x[30].strip(),
                            "cashless": x[31].strip(),
                            "esb": x[31].strip(),
                            "shopee_food": x[32].strip(),

                            
                            "opening_date": x[9].strip(),
                            "opening_date_year": None,
                            "opening_date_month": None,
                            "opening_date_day": None,
                            "opening_date_dayname": None,
                            "opening_date_weeks": None
                            }) 
                        # | 'DeleteIncompleteData DistrictAllocationAndListStore' >> beam.Filter(discard_incomplete_branchessap)
                        | 'ChangeDataType DistrictAllocationAndListStore' >> beam.Map(convert_types_DistrictAllocationAndListStore)
                        # | 'DeleteUnwantedData DistrictAllocationAndListStore' >> beam.Map(del_unwanted_cols_branchessap)
                        | 'Write DistrictAllocationAndListStore' >> WriteToText('output/data-branchessap','.txt')
                        )

    # Write to BQ
    # DistrictAllocationAndListStore_data | 'Write to BQ Tender Daily' >> beam.io.WriteToBigQuery(
    #                 table=table_id_tender,
    #                 dataset=dataset_id,
    #                 project=project_id,
    #                 schema=schema_tenders_master,
    #                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #                 # batch_size=int(100)
    #                 )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
#     # Set the logger
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d:%H:%M:%S',
                        level=logging.INFO)
    # Run the core pipeline
    logging.info('Starting')
    run()

    # Island 1 : sales transaction details >< daily table >< daily order 
    # Island 2 : sales transaction >< sales transaction detail >< item master
    # Island 3 : sales transaction details >< paid >< payment (BERES)
    # Island 4 : sales transaction >< branchessap


    # python py-df-sbst01.py --project=$PROJECT --key=$key --staging_location gs://$PROJECT/starbucks/staging --temp_location gs://$PROJECT//starbucks/temp --worker_region=asia-southeast1 --runner=DataflowRunner --save_main_session --job_name lingga-test-sb
    # python movie_pipeline_cogroup_copy.py --input-basics gs://bucket-lingga/apache-beam/title.basic.100.tsv --output gs://bucket-lingga/apache-beam/movie_test2.txt --input-ratings gs://bucket-lingga/apache-beam/title.rating.100.tsv --project=$PROJECT --region=asia-southeast1 --runner=DataflowRunner --save_main_session --staging_location gs://$PROJECT/staging --temp_location gs://$PROJECT/temp 

    # python py-df-sbst01.py --project=wired-glider-289003 --key=E:/wired-glider-289003-9bd74e62ec18.json --staging_location gs://wired-glider-289003/starbucks/staging --temp_location gs://wired-glider-289003/starbucks/temp --region=asia-southeast1 --runner=DataflowRunner --save_main_session --job_name lingga-test-sb-060821

    # python py-df-sbst01.py --project=wired-glider-289003 --key=E:/wired-glider-289003-9bd74e62ec18.json --staging_location gs://wired-glider-289003/starbucks-BOH/staging --temp_location gs://wired-glider-289003/starbucks-BOH/temp --region=asia-southeast1 --runner=DataflowRunner --save_main_session --job_name lingga-test-sb-boh-strans-branchessap


    # python py-df-sbst01.py --project=wired-glider-289003 --key=E:/wired-glider-289003-9bd74e62ec18.json --staging_location gs://wired-glider-289003/starbucks-BOH/staging --temp_location gs://wired-glider-289003/starbucks-BOH/temp --region=asia-southeast1 --runner=DataflowRunner --save_main_session --job_name lingga-test-sb-boh-strans-branchessap --disk_size_gb=250
    # $env:GOOGLE_APPLICATION_CREDENTIALS="E:/wired-glider-289003-9bd74e62ec18.json"

    # =========================================================================
    #  3 DAYS  Data

    # $env:GOOGLE_APPLICATION_CREDENTIALS="E:/Starbucks/py-df-sbst/ps-id-starbucks-da-05052021-6ca2a1e19a46.json"
    # python py-df-sbst01.py --project=ps-id-starbucks-da-05052021 --key=E:/Starbucks/py-df-sbst/ps-id-starbucks-da-05052021-6ca2a1e19a46.json --staging_location gs://ps-id-starbucks-da-05052021/starbucks-BOH/staging --temp_location gs://ps-id-starbucks-da-05052021/starbucks-BOH/temp --region=asia-southeast2 --runner=DataflowRunner --save_main_session --job_name sb-3d

    # python py-df-sbst01.py --project=ps-id-starbucks-da-05052021 --key=E:/Starbucks/py-df-sbst/ps-id-starbucks-da-05052021-6ca2a1e19a46.json --staging_location gs://ps-id-starbucks-da-05052021/starbucks-BOH/staging --temp_location gs://ps-id-starbucks-da-05052021/starbucks-BOH/temp --region=asia-southeast2 --runner=DataflowRunner --save_main_session --job_name sb-3d --disk_size_gb=150 --max_num_workers=10 --machine_type=n2-standard-4