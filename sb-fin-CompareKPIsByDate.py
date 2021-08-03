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

def convert_types_CompareKPIsByDate(data):
    """Converts string values to their appropriate type."""

    date_format = '%d-%m-%Y'

    data["store_code"] = str(data["store_code"]) if "store_code" in data else None
    data["location"] = str(data['location']) if data.get("location", "") != ""  else None
    data['date'] = str(data['date']) if 'date' in data else None
    data['net_sales'] = round(abs(float(data['net_sales']))) if 'net_sales' in data else None
    data['prep_cost'] = int(data['prep_cost']) if 'prep_cost' in data else None
    data['labor_cost'] = int(data['labor_cost']) if 'labor_cost' in data else None
    data['margin'] = int(data['margin']) if 'margin' in data else None
    data['fc_percent'] = int(data['fc_percent']) if 'fc_percent' in data else None
    data['lc_percent'] = int(data['lc_percent']) if 'lc_percent' in data else None
    data['checks'] = int(data['checks']) if 'checks' in data else None
    data['per_check'] = round(abs(float(data['per_check'])), 2) if 'per_check' in data else None
    data['voids'] = abs(int(data['voids'])) if 'voids' in data else None


    date = datetime.datetime.strptime(data['date'], date_format)
    data['date'] = str(date.date())
    data['date_year'] = str(date.year)
    data['date_month'] = str(date.month)
    data['date_day'] = str(date.day)
    data['date_dayname'] = str(date.strftime("%A"))
    data['date_weeks'] = str(date.strftime("%W"))

    return data

schema_tenders_master = (
    'store_code:STRING,\
    location:STRING,\
    net_sales:INTEGER,\
    prep_cost:INTEGER,\
    labor_cost:FLOAT,\
    margin:INTEGER,\
    fc_percent:INTEGER,\
    lc_percent:INTEGER,\
    checks:INTEGER,\
    per_check:FLOAT,\
    voids:INTEGER,\
    date:DATE,\
    date_year:STRING,\
    date_month:STRING,\
    date_day:STRING,\
    date_dayname:STRING,\
    date_weeks:STRING'
    )

project_id = 'wired-glider-289003'  # replace with your project ID
dataset_id = 'starbuck_data_samples'  # replace with your dataset ID
table_id_tender = 'SB_FIN_CompareKPIsByDate'

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
    CompareKPIsByDate_data = (p 
                        | 'ReadData CompareKPIsByDate data' >> beam.io.ReadFromText('gs://sb_xlsx_data_source/data_output/CompareKPIsByDate.csv', skip_header_lines =1)
                        # | 'ReadData CompareKPIsByDate data' >> beam.io.ReadFromText('data_source_xlxs/data_output_CompareKPIsByDate.csv', skip_header_lines =1)
                        | 'SplitData CompareKPIsByDate' >> beam.Map(lambda x: x.split(',')) #delimiter comma (,)
                        | 'FormatToDict CompareKPIsByDate' >> beam.Map(lambda x: {
                            "store_code": x[0].strip(),
                            "location": x[1].strip(),
                            "date": x[2].strip(),
                            "date_year": None,
                            "date_month": None,
                            "date_day": None,
                            "date_dayname": None,
                            "date_weeks": None,
                            "net_sales": x[3].strip(),
                            "prep_cost": x[4].strip(),
                            "labor_cost": x[5].strip(),
                            "margin": x[6].strip(),
                            "fc_percent": x[7].strip(),
                            "lc_percent": x[8].strip(),
                            "checks": x[9].strip(),
                            "per_check": x[10].strip(),
                            "voids": x[11].strip()
                            }) 
                        # | 'DeleteIncompleteData CompareKPIsByDate' >> beam.Filter(discard_incomplete_branchessap)
                        | 'ChangeDataType CompareKPIsByDate' >> beam.Map(convert_types_CompareKPIsByDate)
                        # | 'DeleteUnwantedData CompareKPIsByDate' >> beam.Map(del_unwanted_cols_branchessap)
                        # | 'Write CompareKPIsByDate' >> WriteToText('output/data-branchessap','.txt')
                        )

    # Write to BQ
    CompareKPIsByDate_data | 'Write to BQ CompareKPIsByDate' >> beam.io.WriteToBigQuery(
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