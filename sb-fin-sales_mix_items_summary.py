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

def convert_types_SalesMixItemsSummary(data):
    """Converts string values to their appropriate type."""

    date_format = "%m/%d/%Y"

    data["item"] = str(data["item"]) if "item" in data else None
    data["family_group"] = str(data["family_group"]) if "family_group" in data else None
    data["major_group"] = str(data["major_group"]) if "major_group" in data else None
    data["gross_sales"] = abs(int(data["gross_sales"])) if data.get("gross_sales").isnumeric() else None
    data["item_discounts"] = abs(int(data["item_discounts"])) if "item_discounts" in data else None
    data["sales_less_item_disc"] = int(data["sales_less_item_disc"]) if "sales_less_item_disc" in data else None
    data["percent_sales"] = round(float(data["percent_sales"]), 2) if "percent_sales" in data else None
    data["qty_sold"] = int(round(float(data["qty_sold"]))) if "qty_sold" in data else None
    data["percent_qty_sold"] = round(float(data["percent_qty_sold"]), 2) if "percent_qty_sold" in data else None
    data["average_price"] = round(float(data["average_price"]), 2) if "average_price" in data else None

    date = datetime.datetime.strptime(data['date'], date_format)
    data['date'] = str(date.date())
    data['date_year'] = str(date.year)
    data['date_month'] = str(date.month)
    data['date_day'] = str(date.day)
    data['date_dayname'] = str(date.strftime("%A"))
    data['date_weeks'] = str(date.strftime("%W"))

    return data

schema_tenders_master = (
    'item:STRING,\
    family_group:STRING,\
    major_group:STRING,\
    gross_sales:INTEGER,\
    item_discounts:INTEGER,\
    sales_less_item_disc:INTEGER,\
    percent_sales:FLOAT,\
    qty_sold:INTEGER,\
    percent_qty_sold:FLOAT,\
    average_price:FLOAT,\
    date:DATE,\
    date_year:STRING,\
    date_month:STRING,\
    date_day:STRING,\
    date_dayname:STRING,\
    date_weeks:STRING'
)

project_id = 'wired-glider-289003'  # replace with your project ID
dataset_id = 'starbuck_data_samples'  # replace with your dataset ID
table_id_tender = 'SB_FIN_SalesMixItemsSummary'

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
    SalesMixItemsSummary_data = (p 
                        | 'ReadData SalesMixItemsSummary data' >> beam.io.ReadFromText('gs://sb_xlsx_data_source/data_output/SalesMixItemsSummary.csv', skip_header_lines =1)
                        # | 'ReadData SalesMixItemsSummary data' >> beam.io.ReadFromText('data_source_xlxs/data_output_SalesMixItemsSummary.csv', skip_header_lines =1)
                        | 'SplitData SalesMixItemsSummary' >> beam.Map(lambda x: x.split(',')) #delimiter comma (,)
                        | 'FormatToDict SalesMixItemsSummary' >> beam.Map(lambda x: {
                            "item": x[0].strip(),
                            "family_group": x[1].strip(),
                            "major_group": x[2].strip(),
                            "gross_sales": x[3].strip(),
                            "item_discounts": x[4].strip(),
                            "sales_less_item_disc": x[5].strip(),
                            "percent_sales": x[6].strip(),
                            "qty_sold": x[7].strip(),
                            "percent_qty_sold": x[8].strip(),
                            "average_price": x[9].strip(),
                            "date": x[10].strip(),
                            }) 
                        # | 'DeleteIncompleteData SalesMixItemsSummary' >> beam.Filter(discard_incomplete_branchessap)
                        | 'ChangeDataType SalesMixItemsSummary' >> beam.Map(convert_types_SalesMixItemsSummary)
                        # | 'DeleteUnwantedData SalesMixItemsSummary' >> beam.Map(del_unwanted_cols_branchessap)
                        # | 'Write SalesMixItemsSummary' >> WriteToText('output/data-branchessap','.txt')
                        )

    # Write to BQ
    SalesMixItemsSummary_data | 'Write to BQ SalesMixItemsSummary' >> beam.io.WriteToBigQuery(
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