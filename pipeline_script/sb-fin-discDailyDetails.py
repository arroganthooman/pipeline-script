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

def convert_types_discdailydetails(data):
    """Converts string values to their appropriate type."""

    date_format = '%m/%d/%Y'

    data["discount_type"] = str(data["discount_type"]) if "discount_type" in data else None
    data["percent"] = round(float(data['percent'])) if data.get("percent", "") != ""  else None
    data['total'] = abs(int(data['total'])) if 'total' in data else None
    data['percent_of_total'] = round(abs(float(data['percent_of_total']))) if 'percent_of_total' in data else None
    data['count'] = int(data['count']) if 'count' in data else None
    data['average'] = round(abs(float(data['average'])), 2) if 'average' in data else None
    date = datetime.datetime.strptime(data['date'], date_format)
    data['date'] = str(date.date())
    data['date_year'] = str(date.year)
    data['date_month'] = str(date.month)
    data['date_day'] = str(date.day)
    data['date_dayname'] = str(date.strftime("%A"))
    data['date_weeks'] = str(date.strftime("%W"))

    return data

schema_tenders_master = (
    'discount_type:STRING,\
    percent:INTEGER,\
    total:INTEGER,\
    percent_of_total:INTEGER,\
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
table_id_tender = 'SB_DISC_DAILY'

# parameters
job_name = "fikri-disc-daily-details" #replace with your job name
temp_location=f'gs://{project_id}/temp'
staging_location = f'gs://{project_id}/starbucks-BOH/staging' # replace with  your folder destination
max_num_workers=3 # replace with preferred num_workers
worker_region='asia-southeast1' #replace with your worker region

def run(argv=None):

    # parser = argparse.ArgumentParser()
    # known_args = parser.parse_known_args(argv)
    # print(known_args)

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
    DiscDailyDetails_data = (p 
                        | 'ReadData DiscDailyDetails data' >> beam.io.ReadFromText('gs://sb_xlsx_data_source/data_output/DiscDailyDetail.csv', skip_header_lines =1)
                        # | 'ReadData DiscDailyDetails data' >> beam.io.ReadFromText('data_source_xlxs/data_output_DiscDailyDetail.csv', skip_header_lines =1)
                        | 'SplitData DiscDailyDetails' >> beam.Map(lambda x: x.split(',')) #delimiter comma (,)
                        | 'FormatToDict DiscDailyDetails' >> beam.Map(lambda x: {
                            "discount_type": x[0].strip(),
                            "percent": x[1].strip(),
                            "total": x[2].strip(),
                            "percent_of_total": x[3].strip(),
                            "count": x[4].strip(),
                            "average": x[5].strip(),
                            "date": x[6].strip(),
                            "date_year": None,
                            "date_month": None,
                            "date_day": None,
                            "date_dayname": None,
                            "date_weeks": None
                            }) 
                        # | 'DeleteIncompleteData DiscDailyDetails' >> beam.Filter(discard_incomplete_branchessap)
                        | 'ChangeDataType DiscDailyDetails' >> beam.Map(convert_types_discdailydetails)
                        # | 'DeleteUnwantedData DiscDailyDetails' >> beam.Map(del_unwanted_cols_branchessap)
                        # | 'Write DiscDailyDetails' >> WriteToText('output/data-branchessap','.txt')
                        )

    # Write to BQ
    DiscDailyDetails_data | 'Write to BQ DiscDailyDetails' >> beam.io.WriteToBigQuery(
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