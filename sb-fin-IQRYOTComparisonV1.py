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

def convert_types(data):
    """Converts string values to their appropriate type."""

    date_format = '%Y-%m-%d'

    data["location_name"] = str(data["location_name"]) if "location_name" in data else None
    data["location_reference"] = str(data['location_reference']) if 'location_reference' in data else None
    data['order_type_name'] = str(data['order_type_name']) if 'order_type_name' in data else None
    data['sales_net_VAT_after_disc'] = abs(int(data['sales_net_VAT_after_disc'])) if 'sales_net_VAT_after_disc' in data else None
    data['store_code'] = str(data['store_code']) if 'store_code' in data else None
    
    date = datetime.datetime.strptime(data['date'], date_format)
    data['date'] = str(date.date())
    data['date_year'] = str(date.year)
    data['date_month'] = str(date.month)
    data['date_day'] = str(date.day)
    data['date_dayname'] = str(date.strftime("%A"))
    data['date_weeks'] = str(date.strftime("%W"))

    return data

schema_tenders_master = (
    'location_name:STRING,\
    location_reference:STRING,\
    order_type_name:STRING,\
    sales_net_VAT_after_disc:INTEGER,\
    store_code:STRING,\
    date:DATE,\
    date_year:STRING,\
    date_month:STRING,\
    date_day:STRING,\
    date_dayname:STRING,\
    date_weeks:STRING'
    )

project_id = 'wired-glider-289003'  # replace with your project ID
dataset_id = 'starbuck_data_samples'  # replace with your dataset ID
table_id_tender = 'SB_IQRYOT_COMPARISON'

# parameters
job_name = "fikri-iqryt-comparison" # replace with your job name
temp_location=f'gs://{project_id}/temp'
staging_location = f'gs://{project_id}/starbucks-BOH/staging' # replace with  your folder destination
max_num_workers=3 # replace with preferred num_workers
worker_region='asia-southeast1' #replace with your worker region


def run(argv=None):
    # parser = argparse.ArgumentParser()
    # known_args = parser.parse_known_args(argv)

    options = PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        job_name=job_name,
        temp_location=temp_location,
        region=worker_region,
        autoscaling_algorithm='THROUGHPUT_BASED',
        max_num_workers=max_num_workers,
        save_main_session = True
    )

    # p = beam.Pipeline(options)
    # p = beam.Pipeline(options=PipelineOptions())

    # pipeline_options = PipelineOptions(pipeline_args)
    # pipeline_options = PipelineOptions(runner)
    # pipeline_options.view_as(SetupOptions).save_main_session = True

    # p = beam.Pipeline(options=PipelineOptions())
    p = beam.Pipeline(options=options)
    
    IQRYOTComparisonV1_data = (p 
                        | 'ReadData IQRYOTComparisonV1 data' >> beam.io.ReadFromText('gs://sb_xlsx_data_source/data_output/IQRYOTComparisonV1.csv', skip_header_lines =1)
                        # | 'ReadData IQRYOTComparisonV1 data' >> beam.io.ReadFromText('data_source_xlxs/data_output_IQRYOTComparisonV1.csv', skip_header_lines =1)
                        | 'SplitData IQRYOTComparisonV1' >> beam.Map(lambda x: x.split(',')) #delimiter comma (,)
                        | 'FormatToDict IQRYOTComparisonV1' >> beam.Map(lambda x: {
                            "location_name": x[1].strip(),
                            "location_reference": x[2].strip(),
                            "order_type_name": x[3].strip(),
                            "sales_net_VAT_after_disc": x[4].strip(),
                            "store_code": x[5].strip(),
                            "date": x[0].strip(),
                            "date_year": None,
                            "date_month": None,
                            "date_day": None,
                            "date_dayname": None,
                            "date_weeks": None
                            }) 
                        # | 'DeleteIncompleteData IQRYOTComparisonV1' >> beam.Filter(discard_incomplete_branchessap)
                        | 'ChangeDataType IQRYOTComparisonV1' >> beam.Map(convert_types)
                        # | 'DeleteUnwantedData IQRYOTComparisonV1' >> beam.Map(del_unwanted_cols_branchessap)
                        # | 'Write IQRYOTComparisonV1' >> WriteToText('output/data-branchessap','.txt')
                        )

    # Write to BQ
    IQRYOTComparisonV1_data | 'Write to BQ IQRYOT Comparison V1' >> beam.io.WriteToBigQuery(
                    table=table_id_tender,
                    dataset=dataset_id,
                    project=project_id,
                    schema=schema_tenders_master,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    # batch_size=int(100)
                    )

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