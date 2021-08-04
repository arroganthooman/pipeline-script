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

def convert_types_SciForecastJune2021(data):
    """Converts string values to their appropriate type."""

    data["site"] = str(data["site"]) if "site" in data else None
    data["store"] = str(data["store"]) if "store" in data else None
    data["city"] = str(data["city"]) if "city" in data else None
    data["type_of_store"] = str(data["type_of_store"]) if "type_of_store" in data else None
    data["om"] = str(data["om"]) if "om" in data else None
    data["dm"] = str(data["dm"]) if "dm" in data else None
    data["total_days"] = int(data["total_days"]) if "total_days" in data else None
    data["sales"] = round(float(data["sales"])) if data.get("sales") else None
    data["trx"] = round(float(data["trx"])) if "trx" in data else None
    data["adt"] = round(float(data["adt"])) if "adt" in data else None
    data["at"] = round(float(data["at"])) if "at" in data else None
    data["month"] = str(data["month"]) if "month" in data else None
    data["year"] = int(data["year"]) if "year" in data else None

    return data

schema_tenders_master = (
    'site:STRING,\
    store:STRING,\
    city:STRING,\
    type_of_store:STRING,\
    om:STRING,\
    dm:STRING,\
    total_days:INTEGER,\
    sales:INTEGER,\
    trx:INTEGER,\
    adt:INTEGER,\
    at:INTEGER,\
    month:STRING,\
    year:INTEGER'
)

project_id = 'wired-glider-289003'  # replace with your project ID
dataset_id = 'starbuck_data_samples'  # replace with your dataset ID
table_id_tender = 'SB_FORCAST'

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
    SciForecastJune2021_data = (p 
                        | 'ReadData SciForecastJune2021 data' >> beam.io.ReadFromText('gs://sb_xlsx_data_source/data_output/sci_forecast_june_2021.csv', skip_header_lines =1)
                        # | 'ReadData SciForecastJune2021 data' >> beam.io.ReadFromText('data_source_xlxs/data_output_DiscDailyDetail.csv', skip_header_lines =1)
                        | 'SplitData SciForecastJune2021' >> beam.Map(lambda x: x.split(',')) #delimiter comma (,)
                        | 'FormatToDict SciForecastJune2021' >> beam.Map(lambda x: {
                            "site": x[0].strip(),
                            "store": x[1].strip(),
                            "city": x[2].strip(),
                            "type_of_store": x[3].strip(),
                            "om": x[4].strip(),
                            "dm": x[5].strip(),
                            "total_days": x[6].strip(),
                            "sales": x[7].strip(),
                            "trx": x[8].strip(),
                            "adt": x[9].strip(),
                            "at": x[10].strip(),
                            "month": x[11].strip(),
                            "year": x[12].strip()
                            }) 
                        # | 'DeleteIncompleteData SciForecastJune2021' >> beam.Filter(discard_incomplete_branchessap)
                        | 'ChangeDataType SciForecastJune2021' >> beam.Map(convert_types_SciForecastJune2021)
                        # | 'DeleteUnwantedData SciForecastJune2021' >> beam.Map(del_unwanted_cols_branchessap)
                        # | 'Write SciForecastJune2021' >> WriteToText('output/data-branchessap','.txt')
                        )

    # Write to BQ
    SciForecastJune2021_data | 'Write to BQ SciForecastJune2021' >> beam.io.WriteToBigQuery(
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