from __future__ import absolute_import

from posixpath import split
import apache_beam as beam
import argparse
import datetime
from decimal import Decimal
from apache_beam.io.textio import ReadAllFromText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import WriteToText
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.coders.coders import Coder
from sys import argv
from datetime import time
import logging
import itertools
from apache_beam.transforms import combiners
from apache_beam.transforms.core import CombineGlobally
from apache_beam.transforms.util import Distinct, RemoveDuplicates

from apache_beam.typehints.decorators import _normalize_var_positional_hint
from apitools.base.py.exceptions import NotFoundError

# CLASS CHANGE FRENCH CHARACTERS
class IgnoreUnicode(Coder):
    def encode(self, value):
        return value.encode('utf-8','ignore')

    def decode(self, value):
        return value.decode('utf-8','ignore')

    def is_deterministic(self):
        return True

# CLASS EXCLUDE ZERO PRICE DORDER DTABLE
class dodt_exclude_zero_price(beam.DoFn):
    def process(self, data):
        try:
            if data['pdisc'] == 1: 
                yield data
            elif data['pdisc'] == 0: 
                if data['price'] != 0:
                    yield data
                elif data['dorder_upref'] != '':
                    yield data
                else: return
        except:
            data['plu'] = 'SCCRM01'
            data['dorder_ordertxt'] = ''
            data['pdisc'] = ''
            data['price'] = ''
            data['dorder_upref'] = ''
            yield data

class ttime_add_emty_data(beam.DoFn):
    def process(self,data):
        try:
            if data['type_trans'] != '':
                if data['timetrans'] != '':
                    yield data
                else:
                    return
        except:
            data['timetrans'] = None
            data['type_trans'] = None
            yield data

class mt_add_empty_data(beam.DoFn):
    def process(self, data):
        try:
            if data['dtype'] != '':
                yield data
            else:
                return
        except:
            data['dtype'] = None
            data['timetrans'] = None
            data['type_trans'] = None
            yield data

class pdisc_add_empty_data(beam.DoFn):
    def process(self, data):
        try:
            if data['disc_time'] != '':
                yield data
            else:
                return
        except:
            data['part_disc'] = False
            data['disc_time'] = None
            data['flag_void'] = None
            data['copy_date'] = None
            data['copy_date_time'] = None
            data['transaction_date'] = None
            yield data

class clean_pay_paymenttype(beam.DoFn):
    def process(self, data):
        try:
            if data['paid_types'] != '':
                yield data
        except:
            data['paid_payment_description'] = ''
            data['paid_types'] = ''
            data['card_code'] = ''
            yield data

# CLASS TO UNNEST MULTIPLE KEY TO JOIN
class Join(beam.PTransform):
    """Composite Transform to implement left/right/inner/outer sql-like joins on
    two PCollections. Columns to join can be a single column or multiple columns"""

    def __init__(self, left_pcol_name, left_pcol, right_pcol_name, right_pcol, join_type, join_keys):
        """
        :param left_pcol_name (str): Name of the first PCollection (left side table in a sql join)
        :param left_pcol (Pcollection): first PCollection
        :param right_pcol_name: Name of the second PCollection (right side table in a sql join)
        :param right_pcol (Pcollection): second PCollection
        :param join_type (str): how to join the two PCollections, must be one of ['left','right','inner','outer']
        :param join_keys (dict): dictionary of two (k,v) pairs, where k is pcol name and
            value is list of column(s) you want to perform join
        """

        self.right_pcol_name = right_pcol_name
        self.left_pcol = left_pcol
        self.left_pcol_name = left_pcol_name
        self.right_pcol = right_pcol
        if not isinstance(join_keys, dict):
            raise TypeError("Column names to join on should be of type dict. Provided one is {}".format(type(join_keys)))
        if not join_keys:
            raise ValueError("Column names to join on is empty. Provide atleast one value")
        elif len(join_keys.keys()) != 2 or set([left_pcol_name, right_pcol_name]) - set(join_keys.keys()):
            raise ValueError("Column names to join should be a dictionary of two (k,v) pairs, where k is pcol name and "
                             "value is list of column(s) you want to perform join")
        else:
            self.join_keys = join_keys
        join_methods = {
            "left": UnnestLeftJoin,
            "right": UnnestRightJoin,
            "inner": UnnestInnerJoin,
            "outer": UnnestOuterJoin,
            "all": UnnestAllJoin
        }
        try:
            self.join_method = join_methods[join_type]
        except KeyError:
            raise Exception("Provided join_type is '{}'. It should be one of {}".format(join_type, join_methods.keys()))

    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, join_keys):
            return [data_dict[key] for key in join_keys], data_dict

        return ({pipeline_name: pcoll
                | 'Convert to ([join_keys], elem) for {}'.format(pipeline_name)
                    >> beam.Map(_format_as_common_key_tuple, self.join_keys[pipeline_name]) for (pipeline_name, pcoll) in pcolls.items()}
                | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                | 'Unnest CoGrouped' >> beam.ParDo(self.join_method(), self.left_pcol_name, self.right_pcol_name)
                )

class UnnestLeftJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records from the left pcol, and the matched records from the right pcol"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        """unnests the record into multiple records, similar to how a left join output is"""
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        for source_dictionary in source_dictionaries:
            if join_dictionaries:
                # For each matching row from the join table, update the source row
                for join_dictionary in join_dictionaries:
                    source_dictionary.update(join_dictionary)
                    yield source_dictionary
            else:
                # if there are no rows matching from the join table, yield the source row as it is
                yield source_dictionary

class UnnestRightJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records from the right pcol, and the matched records from the left pcol"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        """unnests the record into multiple records, similar to how a right join output is"""
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        for join_dictionary in join_dictionaries:
            if source_dictionaries:
                for source_dictionary in source_dictionaries:
                    join_dictionary.update(source_dictionary)
                    yield join_dictionary
            else:
                yield join_dictionary

class UnnestInnerJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records that have matching values in both the pcols"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        """unnests the record into multiple records, similar to how an inner join output is"""
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        if not source_dictionaries or not join_dictionaries:
            pass
        else:
            for source_dictionary in source_dictionaries:
                for join_dictionary in join_dictionaries:
                    source_dictionary.update(join_dictionary)
                    yield source_dictionary

class UnnestOuterJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records when there is a macth in either left pcol or right pcol"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        """unnests the record into multiple records, similar to how an outer join output is"""
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        if source_dictionaries:
            if join_dictionaries:
                for source_dictionary in source_dictionaries:
                    for join_dictionary in join_dictionaries:
                        source_dictionary.update(join_dictionary)
                        yield source_dictionary
            else:
                for source_dictionary in source_dictionaries:
                    yield source_dictionary
        else:
            for join_dictionary in join_dictionaries:
                yield join_dictionary

class UnnestAllJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records when there is a macth in either left pcol or right pcol"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        for source_dictionary in source_dictionaries:
            for join_dictionary in join_dictionaries:
                source_dictionary.update(join_dictionary)
                yield source_dictionary

# FUNCTION REMOVE DUPLICATES
def rmduplicate(data):
    # seen = set() # { (('branch_id', '002'), ('cash_register_id', '001'), ('dorder_ordertxt', ''), ('dorder_upref', ''), ('micros_transaction_number', '0002001-01052021-001794'), ('order_id', 1794), ('pdisc', 0), ('plu', 'SIS102'), ('price', 27000.0), ('sales_date', '2021-05-01'), ('transaction_number', '002001-01052021-1794')), (('branch_id', '436'), ('cash_register_id', '001'), ('dorder_ordertxt', ''), ('dorder_upref', ''), ('micros_transaction_number', '0436001-01052021-001089'), ('order_id', 1089), ('pdisc', 0), ('plu', 'SNW11'), ('price', 27000.0), ('sales_date', '2021-05-01'), ('transaction_number', '436001-01052021-1089')) }
    # for d in data:
    #     t = tuple(sorted(d.items())) # (('branch_id', '094'), ('cash_register_id', '006'), ('dorder_ordertxt', ''), ('dorder_upref', ''), ('micros_transaction_number', '0094006-01052021-006701'), ('order_id', 6701), ('pdisc', 0), ('plu', 'ESPC002'), ('price', 5000.0), ('sales_date', '2021-05-01'), ('transaction_number', '094006-01052021-6701'))
    #     seen.add(t)
    # return list(seen)
    list_data = []
    for d in data:
        t=tuple(sorted(d.items()))
        list_data.append(t)
    
    no_duplicate = set()
    for d in list_data:
        no_duplicate.add(d)

    return list(no_duplicate)

class BreakList(beam.DoFn):
    def process(self, element):
        for e in element:
            yield e

#CONVERT DATA
def convert_types_sales_transaction(data):
    """Converts string values to their appropriate type."""
    date_format = '%Y/%m/%d %H:%M:%S.%f000'
    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None                                          # KEY
    data['st_cashier_id'] = int(data['st_cashier_id']) if 'st_cashier_id' in data else None
    data['st_customer_id'] = str(data['st_customer_id']) if 'st_customer_id' in data else None
    data['st_spending_program_id'] = str(data['st_spending_program_id']) if 'st_spending_program_id' in data else None
    st_trans_date = datetime.datetime.strptime(data['st_transaction_date'], date_format)
    data['st_transaction_date'] = str(st_trans_date.date())                                                                                         # DIMENSION - DATE
    data['st_year'] = str(st_trans_date.year)
    data['st_month'] = str(st_trans_date.month)
    data['st_day'] = str(st_trans_date.day)
    data['st_dayname'] = str(st_trans_date.strftime("%A"))
    data['st_week'] = str(st_trans_date.strftime("%W"))
    data['st_total_discount'] = float(data['st_total_discount']) if 'st_total_discount' in data else None                                           # FACT - SUM
    data['st_cash_register_id'] = int(data['st_cash_register_id']) if 'st_cash_register_id' in data else None
    data['st_total_paid'] = float(data['st_total_paid']) if 'st_total_paid' in data else None                                                       # FACT - SUM
    data['st_net_price'] = float(data['st_net_price']) if 'st_net_price' in data else None                                                          # FACT - SUM
    data['st_tax'] = float(data['st_tax']) if 'st_tax' in data else None                                                                            # FACT - SUM
    data['st_flag_return'] = int(data['st_flag_return']) if 'st_flag_return' in data else None
    data['st_register_return'] = int(data['st_register_return']) if 'st_register_return' in data else None
    st_trans_date_return = datetime.datetime.strptime(data['st_trans_date_return'], date_format)
    data['st_trans_date_return'] = str(st_trans_date_return.date())
    data['st_trans_num_return'] = str(data['st_trans_num_return']) if 'st_trans_num_return' in data else None
    data['st_trans_time'] = str(data['st_trans_time']) if 'st_trans_time' in data else None
    data['st_store_type'] = str(data['st_store_type']) if 'st_store_type' in data else None
    data['st_branch_id'] = str(data['st_branch_id']) if 'st_branch_id' in data else None                                                            # JON KEY TO BRANCHESSAP
    st_receiveon = datetime.datetime.strptime(data['st_receiveon'], date_format)
    data['st_receiveon'] = str(st_receiveon.date())
    data['micros_transaction_number'] = str(data['st_micros_transaction_number']) if 'st_micros_transaction_number' in data else None
    return data

def convert_types_branchessap(data):
    """Converts string values to their appropriate type."""
    data['branch_id'] = str(data['branch_id']) if 'branch_id' in data else None                                                                     # BRANCHESSAP KEY
    data['branch_sap'] = str(data['branch_sap']) if 'branch_sap' in data else None                                                                  # DIMENSION
    data['branch_name'] = str(data['branch_name']) if 'branch_name' in data else None                                                               # DIMENSION
    data['branch_profile'] = int(data['branch_profile']) if 'branch_profile' in data else None                                                      
    return data

def convert_types_paid(data):
    # Converts string values to their appropriate type.
    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None
    data['payment_types'] = int(data['payment_types']) if 'payment_types' in data else None
    data['paid_paid_amount'] = float(data['paid_paid_amount']) if 'paid_paid_amount' in data else None
    # data['paid_payment_description'] = str(data['paid_payment_description']) if 'paid_payment_description' in data else None
    return data

def convert_types_partdics(data):
    """Converts string values to their appropriate type."""
    date_format = '%Y/%m/%d %H:%M:%S.%f000'
    time_format = '%H:%M:%S'
    data['card_no'] = str(data['card_no']) if 'card_no' in data else None
    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None
    disc_time = datetime.datetime.strptime(data['disc_time'],date_format)
    data['disc_time'] = str(disc_time.date())
    data['plu'] = str(data['plu']) if 'plu' in data else None
    data['flag_void'] = int(data['flag_void']) if 'flag_void' in data else None
    copy_date = datetime.datetime.strptime(data['copy_date'],date_format)
    data['copy_date'] = str(copy_date.date())
    data['copy_date_time'] = str(copy_date.time().strftime(time_format))
    data['branch_id'] = str(data['branch_id']) if 'branch_id' in data else None
    transaction_date = datetime.datetime.strptime(data['transaction_date'],date_format)
    data['transaction_date'] = str(transaction_date.date())
    data['part_disc'] = str(data['part_disc']) if 'part_disc' in data else None
    return data

def convert_types_payment_types(data):
    """Converts string values to their appropriate type."""
    data['payment_types'] = int(data['payment_types']) if 'payment_types' in data else None
    data['paid_payment_description'] = str(data['paid_payment_description']) if 'paid_payment_description' in data else None
    data['paid_types'] = str(data['paid_types']) if 'paid_types' in data else None
    data['seq'] = int(data['seq']) if 'Seq' in data else None
    data['card_code'] = str(data['card_code']) if 'card_code' in data else None
    return data

def convert_types_sales_transaction_details(data):
    """Converts string values to their appropriate type."""
    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None                                          # KEY
    data['stdetail_seq'] = int(data['stdetail_seq']) if 'stdetail_seq' in data else None
    data['plu'] = str(data['plu']) if 'plu' in data else None                                                                                       # KEY JOIN TO ITEM
    data['stdetail_item_description'] = str(data['stdetail_item_description']) if 'stdetail_item_description' in data else None
    stdetail_price = Decimal(data['stdetail_price'])
    data['stdetail_price'] = float(stdetail_price) if 'stdetail_price' in data else None                                                            # FACT
    data['stdetail_qty'] = int(data['stdetail_qty']) if 'stdetail_qty' in data else None                                                          # FACT
    stdetail_amount = Decimal(data['stdetail_amount'])
    data['stdetail_amount'] = float(stdetail_amount) if 'stdetail_amount' in data else None                                                         # FACT
    data['stdetail_discount_percentage'] = float(data['stdetail_discount_percentage']) if 'stdetail_discount_percentage' in data else None    
    stdetail_discount_amount = Decimal(data['stdetail_discount_amount'])                                                                            # FACT
    data['stdetail_discount_amount'] = float(stdetail_discount_amount) if 'stdetail_discount_amount' in data else None    
    data['stdetail_extradisc_pct'] = float(data['stdetail_extradisc_pct']) if 'stdetail_extradisc_amt' in data else None
    stdetail_extradisc_amt = Decimal(data['stdetail_extradisc_amt'])
    data['stdetail_extradisc_amt'] = float(stdetail_extradisc_amt) if 'stdetail_extradisc_amt' in data else None
    stdetail_net_price = Decimal(data['stdetail_net_price'])
    data['stdetail_net_price'] = float(stdetail_net_price) if 'stdetail_net_price' in data else None                                                # FACT
    data['stdetail_points_received'] = int(data['stdetail_points_received']) if 'stdetail_points_received' in data else None
    data['stdetail_flag_void'] = int(data['stdetail_flag_void']) if 'stdetail_flag_void' in data else None
    data['stdetail_flag_status'] = int(data['stdetail_flag_status']) if 'stdetail_flag_status' in data else None
    data['stdetail_flag_paket_discount'] = int(data['stdetail_flag_paket_discount']) if 'stdetail_flag_paket_discount' in data else None
    data['stdetail_upsizeto'] = str(data['stdetail_upsizeto']) if 'stdetail_upsizeto' in data else None
    data['micros_transaction_number'] = str(data['micros_transaction_number']) if 'micros_transaction_number' in data else None
    return data

def convert_types_item_master(data):
    """Converts string values to their appropriate type."""
    date_format = '%Y/%m/%d %H:%M:%S.%f000'
    data['Branch_ID'] = str(data['Branch_ID']) if 'Branch_ID' in data else None
    data['plu'] = str(data['plu']) if 'plu' in data else None
    data['Class'] = str(data['Class']) if 'Class' in data else None
    data['item_burui'] = str(data['item_burui']) if 'item_burui' in data else None
    data['DP2'] = str(data['DP2']) if 'DP2' in data else None
    data['Supplier_Code'] = str(data['Supplier_Code']) if 'Supplier_Code' in data else None
    data['SPLU'] = str(data['SPLU']) if 'SPLU' in data else None
    data['item_description'] = str(data['item_description']) if 'item_description' in data else None
    normal_price = Decimal(data['Normal_Price'])
    data['Normal_Price'] = float(normal_price) if 'Normal_Price' in data else None
    current_price = Decimal(data['Current_Price'])
    data['Current_Price'] = str(current_price) if 'Current_Price' in data else None
    disc_percent = Decimal(data['Disc_Percent'])
    data['Disc_Percent'] = float(disc_percent) if 'Disc_Percent' in data else None
    data['MinQty4Disc'] = float(data['MinQty4Disc']) if 'MinQty4Disc' in data else None
    data['Point'] = float(data['Point']) if 'Point' in data else None
    data['Constraint_Qty'] = float(data['Constraint_Qty']) if 'Constraint_Qty' in data else None
    data['Constraint_Amt'] = float(data['Constraint_Amt']) if 'Constraint_Amt' in data else None
    data['Constraint_Point'] = float(data['Constraint_Point']) if 'Constraint_Point' in data else None
    data['Constraint_Flag'] = float(data['Constraint_Flag']) if 'Constraint_Flag' in data else None
    data['Point_Item_Flag'] = float(data['Point_Item_Flag']) if 'Point_Item_Flag' in data else None
    data['Point_Spending_Flag'] = float(data['Point_Spending_Flag']) if 'Point_Spending_Flag' in data else None
    data['Get_Class_Flag'] = float(data['Get_Class_Flag']) if 'Get_Class_Flag' in data else None
    data['Flag'] = int(data['Flag']) if 'Flag' in data else None
    data['Margin'] = float(data['Margin']) if 'Margin' in data else None
    last_update = datetime.datetime.strptime(data['Last_Update'],date_format)
    data['Last_Update'] = str(last_update)
    data['Unit_of_Measure'] = str(data['Unit_of_Measure']) if 'Unit_of_Measure' in data else None
    data['item_article_code'] = str(data['item_article_code']) if 'item_article_code' in data else None
    data['item_long_description'] = str(data['item_long_description']) if 'item_long_description' in data else None
    data['Brand'] = str(data['Brand']) if 'Brand' in data else None
    data['Package'] = float(data['Package']) if 'Package' in data else None
    data['Perishable'] = int(data['Perishable']) if 'Perishable' in data else None
    data['Store_Type'] = int(data['Store_Type']) if 'Store_Type' in data else None
    return data

def convert_types_daily_table(data):
    """Converts string values to their appropriate type."""
    date_format = '%Y/%m/%d %H:%M:%S.%f000'
    dtable_sales_date = datetime.datetime.strptime(data['sales_date'],date_format)
    data['sales_date'] = str(dtable_sales_date.date())                                                                                              # JOINED KEY TO DAILY ORDER
    data['cash_register_id'] = str(data['cash_register_id']) if 'cash_register_id' in data else None                                                # JOINED KEY TO DAILY ORDER
    data['order_id'] = int(data['order_id']) if 'order_id' in data else None                                                                        # JOINED KEY TO DAILY ORDER
    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None                                          # JOINED KEY TO TRANSACTION DETAIL
    data['branch_id'] = str(data['branch_id']) if 'branch_id' in data else None                                                                     # JOINED KEY TO DAILY ORDER
    return data

def convert_types_daily_order(data):
    """Converts string values to their appropriate type."""
    date_format = '%Y/%m/%d %H:%M:%S.%f000'
    dorder_sales_date = datetime.datetime.strptime(data['sales_date'],date_format)
    data['sales_date'] = str(dorder_sales_date.date())                                                                                              # JOINED KEY TO DAILY TABLE
    data['cash_register_id'] = str(data['cash_register_id']) if 'cash_register_id' in data else None                                                # JOINED KEY TO DAILY TABLE
    data['order_id'] = int(data['order_id']) if 'order_id' in data else None                                                                        # JOINED KEY TO DAILY TABLE
    # data['order_idx'] = int(data['order_idx']) if 'order_idx' in data else None
    data['plu'] = str(data['plu']) if 'plu' in data else None
    data['dorder_ordertxt'] = str(data['dorder_ordertxt']) if 'dorder_ordertxt' in data else None                                                   # DIMENSION - FACT CT
    data['pdisc'] = int(data['pdisc']) if 'pdisc' in data else None
    data['price'] = float(data['price']) if 'price' in data else None
    data['dorder_upref'] = str(data['dorder_upref']) if 'dorder_upref' in data else None                                                            # DIMENTION - FACT CT
    data['branch_id'] = str(data['branch_id']) if 'branch_id' in data else None                                                                     # JOINED KEY TO DAILY TABLE
    dorder_receiveon = datetime.datetime.strptime(data['dorder_receiveon'],date_format)
    data['dorder_receiveon'] = str(dorder_receiveon.date())
    return data

def convert_types_transtypemicros(data):
    """Converts string values to their appropriate type."""
    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None
    data['dtype'] = str(data['dtype']) if 'dtype' in data else None
    data['receiveOn'] = str(data['receiveOn']) if 'receiveOn' in data else None
    data['sales_date'] = str(data['sales_date']) if 'sales_date' in data else None
    return data

def convert_types_transtypetime(data):
    """Converts string values to their appropriate type."""
    time_format = '%H:%M:%S.%f000'
    date_format = '%Y-%m-%d'
    datetrans = datetime.datetime.strptime(data['date'], date_format)
    data['date'] = str(datetrans.date())
    time_trans = datetime.datetime.strptime(data['timetrans'],time_format)
    data['timetrans'] = str(time_trans.time())
    datetime_trans = datetime.datetime.combine(datetrans.date(),time_trans.time())
    data['datetime_trans'] = str(datetime_trans)
    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None
    data['type_trans'] = str(data['type_trans']) if 'type_trans' in data else None
    return data

def add_as_dataset(data):
    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None
    data['st_cashier_id'] = int(data['st_cashier_id']) if 'st_cashier_id' in data else None
    data['st_customer_id'] = str(data['st_customer_id']) if 'st_customer_id' in data else None
    data['st_spending_program_id'] = str(data['st_spending_program_id']) if 'st_spending_program_id' in data else None
    data['st_transaction_date'] = str(data['st_transaction_date']) if 'st_transaction_date' in data else None
    data['st_total_discount'] = float(data['st_total_discount']) if 'st_total_discount' in data else None
    data['st_cash_register_id'] = int(data['st_cash_register_id']) if 'st_cash_register_id' in data else None
    data['st_total_paid'] = float(data['st_total_paid']) if 'st_total_paid' in data else None
    data['st_net_price'] = float(data['st_net_price']) if 'st_net_price' in data else None
    data['st_tax'] = float(data['st_tax']) if 'st_tax' in data else None
    data['st_flag_return'] = int(data['st_flag_return']) if 'st_flag_return' in data else None
    data['st_register_return'] = int(data['st_register_return']) if 'st_register_return' in data else None
    data['st_trans_date_return'] = str(data['st_trans_date_return']) if 'st_trans_date_return' in data else None
    data['st_trans_num_return'] = str(data['st_trans_num_return']) if 'st_trans_num_return' in data else None
    data['st_trans_time'] = str(data['st_trans_time']) if 'st_trans_time' in data else None
    data['st_store_type'] = str(data['st_store_type']) if 'st_store_type' in data else None
    data['st_branch_id'] = int(data['st_branch_id']) if 'st_branch_id' in data else None
    data['branch_sap'] = str(data['branch_sap']) if 'branch_sap' in data else None
    data['branch_name'] = str(data['branch_name']) if 'branch_name' in data else None
    data['branch_profile'] = int(data['branch_profile']) if 'branch_profile' in data else None
    data['st_receiveon'] = str(data['st_receiveon']) if 'st_receiveon' in data else None
    data['st_micros_transaction_number'] = str(data['st_micros_transaction_number']) if 'st_micros_transaction_number' in data else None
    data['paid_payment_type'] = int(data['paid_payment_type']) if 'paid_payment_type' in data else None
    data['paid_paid_amount'] = float(data['paid_paid_amount']) if 'paid_paid_amount' in data else None
    data['paid_payment_description'] = str(data['paid_payment_description']) if 'paid_payment_description' in data else None
    data['card_code'] = str(data['card_code']) if 'card_code' in data else None
    data['item_burui'] = str(data['item_burui']) if 'item_burui' in data else None
    data['item_long_description'] = str(data['item_long_description']) if 'item_long_description' in data else None
    data['item_article_code'] = str(data['item_article_code']) if 'item_article_code' in data else None
    data['stdetail_seq'] = int(data['stdetail_seq']) if 'stdetail_seq' in data else None
    data['stdetail_plu'] = str(data['stdetail_plu']) if 'stdetail_plu' in data else None
    data['stdetail_item_description'] = str(data['stdetail_item_description']) if 'stdetail_item_description' in data else None
    data['stdetail_price'] = float(data['stdetail_price']) if 'stdetail_price' in data else None
    data['stdetail_qty'] = float(data['stdetail_qty']) if 'stdetail_qty' in data else None
    data['stdetail_amount'] = float(data['stdetail_amount']) if 'stdetail_amount' in data else None
    data['stdetail_discount_percentage'] = int(data['stdetail_discount_percentage']) if 'stdetail_discount_percentage' in data else None
    data['stdetail_discount_amount'] = float(data['stdetail_discount_amount']) if 'stdetail_discount_amount' in data else None
    data['stdetail_extradisc_pct'] = int(data['stdetail_extradisc_pct']) if 'stdetail_extradisc_pct' in data else None
    data['stdetail_extradisc_amt'] = float(data['stdetail_extradisc_amt']) if 'stdetail_extradisc_amt' in data else None
    data['stdetail_net_price'] = float(data['stdetail_net_price']) if 'stdetail_net_price' in data else None
    data['stdetail_point_recieved'] = int(data['stdetail_point_recieved']) if 'stdetail_point_recieved' in data else None
    data['stdetail_flag_void'] = int(data['stdetail_flag_void']) if 'stdetail_flag_void' in data else None
    data['stdetail_flag_status'] = int(data['stdetail_flag_status']) if 'stdetail_flag_status' in data else None
    data['stdetail_flag_paket_discount'] = int(data['stdetail_flag_paket_discount']) if 'stdetail_flag_paket_discount' in data else None
    data['stdetail_upsizeto'] = str(data['stdetail_upsizeto']) if 'stdetail_upsizeto' in data else None
    data['sales_date'] = str(data['sales_date']) if 'sales_date' in data else None
    data['cash_register_id'] = int(data['cash_register_id']) if 'cash_register_id' in data else None
    data['order_id'] = int(data['order_id']) if 'order_id' in data else None
    data['dorder_ordertxt'] = str(data['dorder_ordertxt']) if 'dorder_ordertxt' in data else None
    data['dorder_upref'] = str(data['dorder_upref']) if 'dorder_upref' in data else None
    data['dorder_receiveon'] = str(data['receiveon']) if 'receiveon' in data else None
    data['dtable_receiveOn'] = str(data['dtable_receiveOn']) if 'dtable_receiveOn' in data else None
    return data

#DELETE OTHER COLUMN
def del_unwanted_cols_sales_transaction(data):
    """Delete the unwanted columns"""
    del data['Card_Number']
    del data['Points_Of_Spending_Program']
    del data['Point_Of_Item_Program']
    del data['Point_Of_Card_Program']
    del data['Payment_Program_ID']
    del data['Net_Amount']
    del data['Change_Amount']
    del data['Flag_Arrange']
    del data['WorkManShip']
    del data['Last_Point']
    del data['Get_Point']
    del data['Status']
    del data['Upload_Status']
    return data

def del_unwanted_cols_branchessap(data):
    """Delete the unwanted columns"""
    del data['branch_profile']
    return data

def del_unwanted_cols_paid(data):
    """Delete the unwanted columns"""
    del data['Seq']
    del data['Currency_ID']
    del data['Currency_Rate']
    del data['Credit_Card_No']
    del data['Credit_Card_Name']
    del data['Shift']
    del data['micros_transaction_number']
    return data  

def del_unwanted_cols_partdisc(data):
    """Delete the unwanted columns"""
    del data['card_no']
    return data

def del_unwanted_cols_payment_types(data):
    """Delete the unwanted columns"""
    del data['seq']
    return data

def del_unwanted_cols_sales_transaction_details(data):
    """Delete the unwanted columns"""
    # del data['Seq']
    # del data['Price']
    # del data['Points_Received']
    # del data['Flag_Status']
    # del data['Flag_Paket_Discount']
    # del data['upsizeTo']
    return data

def del_unwanted_cols_item_master(data):
    """Delete the unwanted columns"""
    del data['Branch_ID']
    del data['Class']
    del data['DP2']
    del data['Supplier_Code']
    del data['SPLU']
    del data['item_description']
    del data['Normal_Price']
    del data['Current_Price']
    del data['Disc_Percent']
    del data['MinQty4Disc']
    del data['Point']
    del data['Constraint_Qty']
    del data['Constraint_Amt'] 
    del data['Constraint_Point']
    del data['Constraint_Flag']
    del data['Point_Item_Flag']
    del data['Point_Spending_Flag']
    del data['Get_Class_Flag']
    del data['Flag']
    del data['Margin']
    del data['Last_Update']
    del data['Unit_of_Measure']
    del data['Brand']
    del data['Package']
    del data['Perishable']
    del data['Store_Type']
    return data

def del_unwanted_cols_daily_table(data):
    """Delete the unwanted columns"""
    del data['numtable']
    del data['status']
    del data['dtype']
    del data['bill_seq']
    del data['cover']
    del data['receiveOn']
    return data

def del_unwanted_cols_daily_order(data):
    """Delete the unwanted columns"""
    del data['order_idx']
    del data['mn_id']
    del data['mn_idx']
    del data['sz_id']
    del data['k_id']
    del data['qty']
    del data['flag_void']
    del data['status']
    del data['printed']
    del data['userid']
    # del data['pdisc']
    del data['pdisc_no']
    del data['upsize']
    del data['upsizeto']
    # del data['price']
    del data['Fp']
    # del data['dorder_receiveon']
    return data

def del_unwanted_cols_transtypemicros(data):
    """Delete the unwanted columns"""
    del data['receiveOn']
    return data

def del_unwanted_cols_transtypetime(data):
    """Delete the unwanted columns"""
    del data['date']
    return data

def del_unwanted_st_bsap_for_dimm(data):
    del data['st_cashier_id']
    del data['st_customer_id']
    del data['st_spending_program_id']
    del data['st_total_discount']
    del data['st_branch_id']
    del data['st_cash_register_id']
    del data['st_total_paid']
    del data['st_net_price']
    del data['st_tax']
    del data['st_flag_return']
    del data['st_register_return']
    del data['st_trans_date_return']
    del data['st_trans_num_return']
    del data['st_trans_time']
    del data['st_store_type']
    del data['st_receiveon']
    del data['micros_transaction_number']
    del data['branch_sap']
    del data['branch_name']
    return data

def del_dorder_dtable_joined(data):
    # del data['sales_date']
    # del data['order_id']
    # del data['cash_register_id']
    # del data['branch_id']
    return data

def del_std_dodt_joined(data):
    # del data['plu']
    # del data['price']
    return data

# =================================
# DELETING UNWANTED AGREGATIONS
def del_stdbaspdtdomtpd_col_agregate(data):
    del data['stdetail_seq']
    del data['stdetail_price']
    del data['stdetail_amount']
    del data['stdetail_discount_percentage']
    del data['stdetail_discount_amount']
    del data['stdetail_extradisc_pct']
    del data['stdetail_extradisc_amt']
    del data['stdetail_net_price']
    del data['stdetail_points_received']
    del data['price']
    return data

# =================================
# FILTERING
def typemicros_receiveon_isnotNULL(data):
    return data['receiveOn'] != 'NULL'

# Parameters Required
project_id = 'wired-glider-289003'
dataset_id = 'starbuck_data_samples'
table_id_fact_stdetail = 'fact_stdetail_test_machine'

temp_location = 'gs://wired-glider-289003/temp'
worker_region='asia-southeast1'
max_num_workers=20

# Table Reference Setting
table_spec_std_fact = bigquery.TableReference(
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table_id_fact_stdetail)

#TABLE SCHEMA
table_schema_stdetail_fact = 'transaction_number:STRING,\
    stdetail_seq:INTEGER,\
    plu:STRING,\
    stdetail_item_description:STRING,\
    stdetail_price:FLOAT,\
    stdetail_qty:FLOAT,\
    stdetail_amount:FLOAT,\
    stdetail_discount_percentage:FLOAT,\
    stdetail_discount_amount:FLOAT,\
    stdetail_extradisc_pct:FLOAT,\
    stdetail_extradisc_amt:FLOAT,\
    stdetail_net_price:FLOAT,\
    stdetail_point_received:INTEGER,\
    stdetail_flag_void:INTEGER,\
    stdetail_flag_status:INTEGER,\
    stdetail_flag_paket_discount:INTEGER,\
    stdetail_upsizeto:STRING,\
    micros_transaction_number:STRING,\
    part_disc:BOOLEAN,\
    st_transaction_date:DATE,\
    st_year:STRING,\
    st_month:STRING,\
    st_day:STRING,\
    st_dayname:STRING,\
    st_week:STRING,\
    branch_id:STRING,\
    sales_date:DATE,\
    cash_register_id:STRING,\
    order_id:INTEGER,\
    dorder_ordertxt:STRING,\
    pdisc:INTEGER,\
    price:FLOAT,\
    dorder_upref:STRING,\
    dorder_receiveon:DATE,\
    dtype:STRING,\
    timetrans:TIME,\
    datetime_trans:DATETIME,\
    type_trans:STRING,\
    disc_time:DATE,\
    flag_void:INTEGER,\
    copy_date:DATE,\
    copy_date_time:TIME,\
    transaction_date:DATE'

# datasourcesParameters
data_location = 'gs://sb-data-source/data_source_20210501/'
# data_location = 'gs://sb-data-source/1m_datasource_20210501/'
# data_location = 'gs://sb-data-source/BOH_2021_05/'
# data_location = 'data_source/'
# data_location = 'data_source/BOH_2021_05/'

# 1 Day Data SOURCE
st = data_location + 'sales_transactions_20210607_094416.txt'
stdetail = data_location +'sales_transaction_details_20210607_094608.txt'
branchessap = data_location + 'BranchesSap_20210607_093034.txt'
# paid = data_location + 'paid_20210607_095120.txt'
# payment_type = data_location + 'Payment_Types_20210607_093448.txt'
part_disc = data_location + 'part_disc_20210607_095311.txt'
# item_master = data_location +'item_master_20210607_093216.txt'
dorder = data_location +'daily_order_20210607_094920.txt'
dtable = data_location +'daily_table_20210607_094807.txt'
type_micros = data_location + 'Transaction_Type_Micros_20210611_124914.txt'
type_time = data_location + 'Transaction_Type_Time_20210611_124917.txt'

#1 Month Data Source
# st = data_location + 'sales_transactions_20210728_132936.txt'
# stdetail = data_location +'sales_transaction_details_20210728_133857.txt'
# branchessap = data_location + 'BranchesSap_20210728_140524.txt'
# paid = data_location + 'paid_20210728_140218.txt'
# payment_type = data_location + 'Payment_Types_20210728_140501.txt'
# part_disc = data_location + 'part_disc_20210728_140151.txt'
# item_master = data_location +'item_master_20210728_140512.txt'
# dorder = data_location +'daily_order_20210728_140624.txt'
# dtable = data_location +'daily_table_20210728_140540.txt'
# type_micros = data_location + 'Transaction_Type_Micros_20210730_090844.txt'
# type_time = data_location + 'Transaction_Type_Time_20210730_090714.txt'

#1st DATA FEED 01 - 10
# st = data_location + '2021_05_01_10/sales_transactions_20210801_210110.txt'
# stdetail = data_location +'2021_05_01_10/sales_transaction_details_20210801_210422.txt'
# branchessap = data_location + '2021_05_01_10/BranchesSap_20210801_211616.txt'
# # paid = data_location + '2021_05_01_10/paid_20210801_210652.txt'
# # payment_type = data_location + '2021_05_01_10/Payment_Types_20210801_211616.txt'
# part_disc = data_location + '2021_05_01_10/part_disc_20210801_210649.txt'
# # item_master = data_location +'2021_05_01_10/item_master_20210801_211616.txt'
# dorder = data_location +'2021_05_01_10/daily_order_20210801_211102.txt'
# dtable = data_location +'2021_05_01_10/daily_table_20210801_211053.txt'
# type_micros = data_location + '2021_05_01_10/Transaction_Type_Micros_202108021.txt'
# type_time = data_location + '2021_05_01_10/Transaction_Type_Time_202108021.txt'

#2nd DATA FEED 11 - 20
# st = data_location + '2021_05_11_20/sales_transactions_20210801_212107.txt'
# stdetail = data_location +'2021_05_11_20/sales_transaction_details_20210801_212400.txt'
# branchessap = data_location + '2021_05_11_20/BranchesSap_20210801_213849.txt'
# # paid = data_location + '2021_05_11_20/paid_20210801_212645.txt'
# # payment_type = data_location + '2021_05_11_20/Payment_Types_20210801_213848.txt'
# part_disc = data_location + '2021_05_11_20/part_disc_20210801_212643.txt'
# # item_master = data_location +'2021_05_11_20/item_master_20210801_213849.txt'
# dorder = data_location +'2021_05_11_20/daily_order_20210801_212949.txt'
# dtable = data_location +'2021_05_11_20/daily_table_20210801_212940.txt'
# type_micros = data_location + '2021_05_11_20/Transaction_Type_Micros_202108023.txt'
# type_time = data_location + '2021_05_11_20/Transaction_Type_Time_202108023.txt'

#3rd DATA FEED 21 - 31
# st = data_location + '2021_05_21_31/sales_transactions_20210801_214416.txt'
# stdetail = data_location +'2021_05_21_31/sales_transaction_details_20210801_214652.txt'
# branchessap = data_location + '2021_05_21_31/BranchesSap_20210801_220059.txt'
# # paid = data_location + '2021_05_21_31/paid_20210801_214937.txt'
# # payment_type = data_location + '2021_05_21_31/Payment_Types_20210801_220058.txt'
# part_disc = data_location + '2021_05_21_31/part_disc_20210801_214935.txt'
# # item_master = data_location +'2021_05_21_31/item_master_20210801_220058.txt'
# dorder = data_location +'2021_05_21_31/daily_order_20210801_215211.txt'
# dtable = data_location +'2021_05_21_31/daily_table_20210801_215201.txt'
# type_micros = data_location + '2021_05_21_31/Transaction_Type_Micros_202108022.txt'
# type_time = data_location + '2021_05_21_31/Transaction_Type_Time_202108022.txt'

def run(argv=None):
    parser = argparse.ArgumentParser()
    args, pipeline_arg = parser.parse_known_args(argv)
    
    # Pipeline Options Settings
    # pipeline_options=PipelineOptions(
    #     pipeline_arg,
    #     runner='DataflowRunner',
    #     project=project_id,
    #     job_name='ivantest-std-fact-1day-2021-05-01',
    #     temp_location=temp_location,
    #     # flexrs_goal='COST_OPTIMIZED',
    #     region=worker_region,
    #     autoscaling_algorithm='THROUGHPUT_BASED',
    #     machine_type='n1-standard-2',
    #     # machine_type='n1-standard-8',
    #     disk_size_gb=100,
    #     max_num_workers=max_num_workers
    # )
    # pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options = PipelineOptions()

    #Pipeline Running
    p = beam.Pipeline(options=pipeline_options)
    sales_transaction_data = (p | 'ReadData transaction' >> beam.io.ReadFromText(st, skip_header_lines =1)
                            | 'SplitData transaction' >> beam.Map(lambda x: x.split('|'))
                            | 'FormatToDict transaction' >> beam.Map(lambda x: {"transaction_number": x[0], "st_cashier_id": x[1], "st_customer_id": x[2].strip(), "Card_Number": x[3], "st_spending_program_id": x[4].strip(), "st_transaction_date": x[5], "st_year": None, "st_month": None, "st_day": None, "st_dayname": None, "st_week": None, "st_total_discount": x[6], "Points_Of_Spending_Program": x[7], "Point_Of_Item_Program": x[8], "Point_Of_Card_Program": x[9],"Payment_Program_ID": x[10].strip(),"st_branch_id": x[11],"st_cash_register_id": x[12],"st_total_paid": x[13],"st_net_price": x[14],"st_tax": x[15],"Net_Amount": x[16],"Change_Amount": x[17],"Flag_Arrange": x[18].strip(),"WorkManShip": x[19],"st_flag_return": x[20],"st_register_return": x[21],"st_trans_date_return": x[22].strip(),"st_trans_num_return": x[23].strip(),"Last_Point": x[24],"Get_Point": x[25],"Status": x[26],"Upload_Status": x[27].strip(),"st_trans_time": x[28].strip(),"st_store_type": x[29].strip(),"st_receiveon": x[30],"micros_transaction_number": x[31]})
                            | 'ChangeDataType transaction' >> beam.Map(convert_types_sales_transaction)
                            | 'DeleteUnwantedData transaction' >> beam.Map(del_unwanted_cols_sales_transaction)
    )
    
    branchessap_data = (p | 'ReadData branchessap' >> beam.io.ReadFromText(branchessap, skip_header_lines =1, coder=IgnoreUnicode())
                            | 'SplitData branchessap' >> beam.Map(lambda x: x.split('|'))
                            | 'FormatToDict branchessap' >> beam.Map(lambda x: {"branch_id": x[0], "branch_sap": x[1], "branch_name": x[2], "branch_profile": x[3]})
                            | 'ChangeDataType branchessap' >> beam.Map(convert_types_branchessap)
                            | 'DELETE UNWANTED DATA BRANCHESSAP' >> beam.Map(del_unwanted_cols_branchessap)
    )

    # paid_data = (p | 'ReadData paid' >> beam.io.ReadFromText(paid, skip_header_lines =1, coder=IgnoreUnicode())
    #                         | 'SplitData paid' >> beam.Map(lambda x: x.split('|'))
    #                         | 'FormatToDict paid' >> beam.Map(lambda x: {"transaction_number": x[0], "payment_types": x[1], "Seq": x[2], "Currency_ID": x[3], "Currency_Rate": x[4], "Credit_Card_No": x[5], "Credit_Card_Name": x[6], "paid_paid_amount": x[7], "Shift": x[8], "micros_transaction_number": x[9]})
    #                         | 'ChangeDataType paid' >> beam.Map(convert_types_paid)
    #                         | 'DeleteUnwantedData paid' >> beam.Map(del_unwanted_cols_paid)
    # )

    # payment_type_data = (p | 'ReadData payment type' >> beam.io.ReadFromText(payment_type, skip_header_lines =1, coder=IgnoreUnicode())
    #                         | 'SplitData payment type' >> beam.Map(lambda x: x.split('|'))
    #                         | 'FormatToDict payment type' >> beam.Map(lambda x: {"payment_types": x[0], "paid_payment_description": x[1].strip(), "paid_types": x[2].strip(), "seq": x[3], "card_code": x[4].strip()})
    #                         | 'ChangeDataType payment type' >> beam.Map(convert_types_payment_types)
    #                         | 'DeleteUnwantedData payment type' >> beam.Map(del_unwanted_cols_payment_types)
    # )

    st_details_data = (p | 'ReadData transaction details' >> beam.io.ReadFromText(stdetail, skip_header_lines =1, coder=IgnoreUnicode())
                            | 'SplitData transaction details' >> beam.Map(lambda x: x.split('|'))
                            | 'FormatToDict transaction details' >> beam.Map(lambda x: {"transaction_number": x[0], "stdetail_seq": x[1], "plu": x[2].strip(), "stdetail_item_description": x[3], "stdetail_price": x[4], "stdetail_qty": x[5], "stdetail_amount": x[6], "stdetail_discount_percentage": x[7], "stdetail_discount_amount": x[8], "stdetail_extradisc_pct": x[9],"stdetail_extradisc_amt": x[10],"stdetail_net_price": x[11],"stdetail_points_received": x[12],"stdetail_flag_void": x[13],"stdetail_flag_status": x[14],"stdetail_flag_paket_discount": x[15],"stdetail_upsizeto": x[16],"micros_transaction_number": x[17], "part_disc":''})
                            | 'ChangeDataType transaction details' >> beam.Map(convert_types_sales_transaction_details)
                            # | 'DeleteUnwantedData transaction details' >> beam.Map(del_unwanted_cols_sales_transaction_details)
    )

    # item_master_data = (p | 'ReadData item master' >> beam.io.ReadFromText(item_master, skip_header_lines =1, coder=IgnoreUnicode())
    #                         | 'SplitData item master' >> beam.Map(lambda x: x.split('|'))
    #                         | 'FormatToDict item master' >> beam.Map(lambda x: {"Branch_ID": x[0], "plu": x[1].strip(), "Class": x[2], "item_burui": x[3], "DP2": x[4], "Supplier_Code": x[5], "SPLU": x[6], "item_description": x[7], "Normal_Price": x[8], "Current_Price": x[9], "Disc_Percent": x[10], "MinQty4Disc": x[11], "Point": x[12], "Constraint_Qty": x[13], "Constraint_Amt": x[14], "Constraint_Point": x[15], "Constraint_Flag": x[16], "Point_Item_Flag": x[17], "Point_Spending_Flag": x[18], "Get_Class_Flag": x[19], "Flag": x[20], "Margin": x[21], "Last_Update": x[22], "Unit_of_Measure": x[23], "item_article_code": x[24], "item_long_description": x[25], "Brand": x[26], "Package": x[27], "Perishable": x[28], "Store_Type": x[29]})
    #                         | 'ChangeDataType item master' >> beam.Map(convert_types_item_master)
    #                         | 'DeleteUnwantedData item master' >> beam.Map(del_unwanted_cols_item_master)
    # )
        
    daily_table_data = (p | 'ReadData daily table' >> beam.io.ReadFromText(dtable, skip_header_lines =1, coder=IgnoreUnicode())
                            | 'Remove Duplicates DTABLE' >> beam.Distinct()
                            | 'SplitData daily table' >> beam.Map(lambda x: x.split('|'))
                            | 'FormatToDict daily table' >> beam.Map(lambda x: {"sales_date": x[0], "cash_register_id": x[1],  "order_id": x[2], "numtable": x[3], "status": x[4], "dtype": x[5], "transaction_number": x[6], "bill_seq": x[7], "cover": x[8], "branch_id": x[9], "receiveOn": x[10], "micros_transaction_number": x[11]})
                            | 'ChangeDataType daily table' >> beam.Map(convert_types_daily_table)
                            | 'DeleteUnwantedData daily table' >> beam.Map(del_unwanted_cols_daily_table)
    )
        
    daily_order_data = (p | 'ReadData daily order' >> beam.io.ReadFromText(dorder, skip_header_lines =1, coder=IgnoreUnicode())
                            | 'SplitData daily order' >> beam.Map(lambda x: x.split('|'))
                            | 'FormatToDict daily order' >> beam.Map(lambda x: {"sales_date": x[0], "cash_register_id": x[1], "order_id": x[2], "order_idx": x[3], "mn_id": x[4], "mn_idx": x[5], "sz_id": x[6], "k_id": x[7], "plu": x[8].strip(), "qty": x[9], "flag_void": x[10], "status": x[11], "printed": x[12], "dorder_ordertxt": x[13], "userid": x[14], "pdisc": x[15], "pdisc_no": x[16], "upsize": x[17], "upsizeto": x[18], "price": x[19], "Fp": x[20], "dorder_upref": x[21].strip(), "branch_id": x[22], "dorder_receiveon": x[23]})
                            | 'ChangeDataType daily order' >> beam.Map(convert_types_daily_order)
                            | 'DeleteUnwantedData daily order' >> beam.Map(del_unwanted_cols_daily_order)
    )

    transtypemicros_data = (p | 'ReadData transtype micros' >> beam.io.ReadFromText(type_micros, skip_header_lines =1, coder=IgnoreUnicode())
                            | 'SplitData transtype micros' >> beam.Map(lambda x: x.split('|'))
                            | 'FormatToDict transtype micros' >> beam.Map(lambda x: {"transaction_number": x[0].strip(), "dtype": x[1].strip(), "receiveOn": x[2].strip(), "sales_date": x[3].strip()}) 
                            | 'ChangeDataType transtype micros' >> beam.Map(convert_types_transtypemicros)
                            | 'DeleteUnwantedData transtype micros' >> beam.Map(del_unwanted_cols_transtypemicros)
    )

    rm_transtypemicros_data = (transtypemicros_data | 'CHANGE TRANS TYPE MICROS INTO LIST' >> beam.combiners.ToList()
                                                    | 'REMOVE DUPLICATE IN TRANS TYPE MICROS' >> beam.Map(rmduplicate)
                                                    | 'CHANGE LIST TRANS TYPE MICROS INTO DICTIONARY' >> beam.ParDo(BreakList())
    )

    transtypetime_data = (p | 'ReadData transtype tipe' >> beam.io.ReadFromText(type_time, skip_header_lines =1, coder=IgnoreUnicode())
                            | 'SplitData transtype tipe' >> beam.Map(lambda x: x.split('|'))
                            | 'FormatToDict transtype tipe' >> beam.Map(lambda x: {"date": x[0].strip(), "timetrans": x[1].strip(), "datetime_trans": None, "transaction_number": x[2].strip(), "type_trans": x[3].strip()}) 
                            | 'ChangeDataType transtype tipe' >> beam.Map(convert_types_transtypetime)
                            | 'DeleteUnwantedData transtype tipe' >> beam.Map(del_unwanted_cols_transtypetime)
    )

    partdics_data = (p | 'ReadData partdisc' >> beam.io.ReadFromText(part_disc, skip_header_lines =1, coder=IgnoreUnicode())
                            | 'SplitData partdisc' >> beam.Map(lambda x: x.split('|'))                    
                            | 'FormatToDict partdisc' >> beam.Map(lambda x: {"card_no": x[0].strip(), "transaction_number": x[1].strip(), "disc_time": x[2].strip(), "plu": x[3].strip(), "flag_void": x[4].strip(), "copy_date": x[5].strip(), "copy_date_time": None, "branch_id": x[6].strip(), "transaction_date": x[7].strip(), "part_disc": True})
                            | 'ChangeDataType partdisc' >> beam.Map(convert_types_partdics)
                            | 'DeleteUnwantedData partdisc' >> beam.Map(del_unwanted_cols_partdisc)
    )

    st_data = 'sales_transaction_data'
    bsap_data = 'branchessap_data'
    # payment_data = 'paid_data'
    # paytype_data = 'payment_type_data'
    stdetail_data = 'st_details_data'
    # item_data = 'item_master_data'
    dtable_data = 'daily_table_data'
    dorder_data = 'daily_order_data'
    tmicros_data = 'rm_transtypemicros_data'
    ttime_data = 'transtypetime_data'
    
    #JOINED KEYS
    join_st_bsap = {
        st_data: ['st_branch_id'],
        bsap_data: ['branch_id']
    }

    # join_paid_paytype = {
    #     payment_data: ['payment_types'],
    #     paytype_data: ['payment_types']
    # }

    # join_stdetail_item = {
    #     stdetail_data: ['plu'],
    #     item_data: ['plu']
    # }

    join_four_keys = {
        dtable_data: ['branch_id','sales_date','cash_register_id','order_id'],
        dorder_data: ['branch_id','sales_date','cash_register_id','order_id'] 
    }

    join_micro_time = {
        tmicros_data: ['transaction_number'],
        ttime_data: ['transaction_number']
    }

    st_branchessap_dict = {st_data:sales_transaction_data, bsap_data: branchessap_data}
    # paid_paytype_dict = {payment_data:paid_data, paytype_data:payment_type_data}
    # stdetail_item_dict = {stdetail_data: st_details_data, item_data: item_master_data}
    dtdo_dict = {dtable_data: daily_table_data, dorder_data: daily_order_data}
    tmicros_time_dict = {tmicros_data:rm_transtypemicros_data, ttime_data:transtypetime_data}

    # JOINED DIMENSION
    join_dim_st_bsap = (st_branchessap_dict | 'LEFT JOIN ST >< BRANCESES_SAP DIMM' >> Join(left_pcol_name=st_data, left_pcol=sales_transaction_data,
                                                                                     right_pcol_name=bsap_data, right_pcol=branchessap_data,
                                                                                     join_type='left', join_keys=join_st_bsap)
                                            | 'GET ST >< BRANCES_SAP DIMM' >> beam.Map(del_unwanted_st_bsap_for_dimm)
    )

    # JOINED PCOLLECTIONS
    # joined_paid_paytype = (paid_paytype_dict | 'LEFT JOIN PAID >< PAYMENT_TYPE' >> Join(left_pcol_name=payment_data, left_pcol=paid_data,
    #                                                                                  right_pcol_name=paytype_data, right_pcol=payment_type_data,
    #                                                                                  join_type='left', join_keys=join_paid_paytype)

    # )

    # cleaned_pay_payttpe = (joined_paid_paytype | 'CLEAN UNRECOGNISE PAYMENT TYPE' >> beam.ParDo(clean_pay_paymenttype()))

    # left_join_std_item = (stdetail_item_dict | 'Left Join Stdetail Item Master' >> Join(left_pcol_name=stdetail_data, left_pcol=st_details_data,
    #                                                                                     right_pcol_name=item_data, right_pcol=item_master_data,
    #                                                                                     join_type='left', join_keys=join_stdetail_item)
    # )

    left_join_dtdo = (dtdo_dict | 'Left Join DOrder DTable' >> Join(left_pcol_name=dtable_data, left_pcol=daily_table_data,
                                                                    right_pcol_name=dorder_data, right_pcol=daily_order_data,
                                                                    join_type='left', join_keys=join_four_keys)
                                | 'Filter Zero price value DOrder DTable' >> beam.ParDo(dodt_exclude_zero_price())
    )

    left_join_micro_time = (tmicros_time_dict | 'Left Join Type Micros Type Time' >> Join(left_pcol_name=tmicros_data, left_pcol=rm_transtypemicros_data,
                                                                    right_pcol_name=ttime_data, right_pcol=transtypetime_data,
                                                                    join_type='left', join_keys=join_micro_time)
                                                | 'ADD EMPTY DATA FOR TYPE TIME' >> beam.ParDo(ttime_add_emty_data())
    )

    #CREATE JOIN STDETAIL BSAP DIMM
    st_bsap_dimm = 'join_dim_st_bsap'
    join_std_bsap_key = {
        stdetail_data: ['transaction_number'],
        st_bsap_dimm: ['transaction_number']
    }
    std_bsap_dim_dict = {stdetail_data:st_details_data,st_bsap_dimm:join_dim_st_bsap}

    # JOINED STD BSAP
    left_join_std_bsap = (std_bsap_dim_dict | 'Left Join STD BSAP DIM' >> Join(left_pcol_name=stdetail_data, left_pcol=st_details_data,
                                                                    right_pcol_name=st_bsap_dimm, right_pcol=join_dim_st_bsap,
                                                                    join_type='left', join_keys=join_std_bsap_key)
    )

    # CREATE JOIN STDETAIL BSAP DIM DTABLE DORDER KEY
    stdbsap_dimm_data = 'left_join_std_bsap'
    dtdo_data = 'left_join_dtdo'
    join_keys = {
        stdbsap_dimm_data: ['transaction_number','plu'],
        dtdo_data: ['transaction_number','plu']
    }
    join_stddtdo_dict = {stdbsap_dimm_data: left_join_std_bsap, dtdo_data: left_join_dtdo}

    # JOINED PCOLLECTIONS STDETAIL DTABLE DORDER
    left_join_std_dtdo = (join_stddtdo_dict | 'Left Join STD DTable DOrder' >> Join(left_pcol_name=stdbsap_dimm_data, left_pcol=left_join_std_bsap,
                                                                    right_pcol_name=dtdo_data, right_pcol=left_join_dtdo,
                                                                    join_type='left', join_keys=join_keys)
    )

    print("test")
    print(left_join_dtdo)
    print("test2")
    
    rm_left_std_dtdo = (left_join_std_dtdo | 'CHANGE JOINED STD DTDO INTO LIST' >> beam.combiners.ToList() 
                                            | 'REMOVE DUPLICATE IN STD DTDO' >> beam.Map(rmduplicate)
                                            | 'CHANGE LIST STD DTDO INTO DICTIONARY' >> beam.ParDo(BreakList())
    )


    # CREATE JOIN STDETAIL BSAP DIM DTABLE DORDER MICRO TIME KEY
    stdbaspdtdo_microtime_data = 'rm_left_std_dtdo'
    microtime_data = 'left_join_micro_time'
    stdbaspdtdomt_key = {
        stdbaspdtdo_microtime_data: ['transaction_number'],
        microtime_data: ['transaction_number']
    }
    join_stdbaspdtdomt_dict = {stdbaspdtdo_microtime_data: rm_left_std_dtdo, microtime_data: left_join_micro_time}

    # JOINED PCOLLECTIONS STDETAIL DTABLE DORDER MICRO TIME
    left_join_stdbaspdtdomt = (join_stdbaspdtdomt_dict | 'Left Join STD DTDO MICRO TIME' >> Join(left_pcol_name=stdbaspdtdo_microtime_data, left_pcol=rm_left_std_dtdo,
                                                                    right_pcol_name=microtime_data, right_pcol=left_join_micro_time,
                                                                    join_type='left', join_keys=stdbaspdtdomt_key)
                                                        | 'ADD EMPTY DTYPE TRANS' >> beam.ParDo(mt_add_empty_data())
    )

    # CREATE JOIN STDETAIL BSAP DIM DTABLE DORDER MICRO TIME PART DISCOUNT KEY
    stdbaspdtdomt_pd_data = 'left_join_stdbaspdtdomt'
    pd_data = 'partdics_data'
    stdbaspdtdomtpd_key = {
        stdbaspdtdomt_pd_data: ['transaction_number','plu','branch_id'],
        pd_data: ['transaction_number','plu','branch_id']
    }
    join_stdbaspdtdomtpd_dict = {stdbaspdtdomt_pd_data: left_join_stdbaspdtdomt, pd_data: partdics_data}

    # JOINED PCOLLECTIONS STDETAIL DTABLE DORDER MICRO TIME PART DISCOUNT
    left_join_stdbaspdtdomtpd = (join_stdbaspdtdomtpd_dict | 'Left Join STD DTDO MICRO TIME PART DISC' >> Join(left_pcol_name=stdbaspdtdomt_pd_data, left_pcol=left_join_stdbaspdtdomt,
                                                                    right_pcol_name=pd_data, right_pcol=partdics_data,
                                                                    join_type='left', join_keys=stdbaspdtdomtpd_key)
                                                            | 'ADD EMPTY DATE FOR PART DISCOUNT' >> beam.ParDo(pdisc_add_empty_data())
    )

    # stdetail_fact = (left_join_stdbaspdtdomtpd  | 'toLIST STDETAIL FACT' >> beam.combiners.ToList()
    #                                             | 'REMOVE DUPLICATE STDETAIL FACT' >> beam.Map(rmduplicate)
    #                                             | 'toDict STDETAIL FACT' >> beam.ParDo(BreakList())
    # )

    # PAID ITEM
    # cleaned_pay_payttpe_data = 'cleaned_pay_payttpe'
    # stdbaspdtdomtpd_data = 'left_join_stdbaspdtdomtpd'
    # pp_stdbaspdtdomtpd_key = {
    #     cleaned_pay_payttpe_data: ['transaction_number'],
    #     stdbaspdtdomtpd_data: ['transaction_number']
    # }
    # join_ppstdbaspdtdomtpd_dict = {cleaned_pay_payttpe_data: cleaned_pay_payttpe, stdbaspdtdomtpd_data: left_join_stdbaspdtdomtpd}

    # left_join_ppstdbaspdtdomtpd = (join_ppstdbaspdtdomtpd_dict | 'Left Join PAID PAY STD DTDO MICRO TIME PART DISC' >> Join(left_pcol_name=cleaned_pay_payttpe_data, left_pcol=cleaned_pay_payttpe,
    #                                                                 right_pcol_name=stdbaspdtdomtpd_data, right_pcol=left_join_stdbaspdtdomtpd,
    #                                                                 join_type='left', join_keys=pp_stdbaspdtdomtpd_key)
    # )

    # EXECUTE to Local TEXT
    # clean_pay_payttpe | 'WriteToText PAID >< PAYTYPE' >> beam.io.WriteToText('data_output/772021_paid_paytype_cleaned','.txt')
    # rm_transtypemicros_data | 'WriteToText TRANS TYPE MICROS' >> beam.io.WriteToText('data_output/1272021_rm_transtypemicros','.txt')
    # left_join_micro_time | 'WriteToText TRANS TYPE MICROS TYPE TIME' >> beam.io.WriteToText('data_output/1272021_2_left_tmicros_ttime','.txt')
    # rm_left_std_dtdo | 'WriteToText STD BASP PAID DTDO FILTERED REMOVE' >> beam.io.WriteToText('data_output/772021_filtered_rm7_std_dtdo_data','.txt')
    # rm_left_std_dtdo | 'WriteToText STD BASP PAID DTDO FILTERED REMOVE' >> beam.io.WriteToText('data_output/30072021_1_std_dtdo_data','.txt')
    left_join_std_dtdo | 'WriteToText STD BASP PAID DTDO FILTERED REMOVE' >> beam.io.WriteToText('data_output/30072021_std_dtdo_data','.txt')
    # left_join_stdbaspdtdomt | 'WriteToText STDETAIL DTABLE DORDER MICROS TIME' >> beam.io.WriteToText('data_output/1472021_5_left_stdbaspdtdo_mt','.txt')
    # partdics_data | 'WriteToText PART DISCOUNT' >> beam.io.WriteToText('data_output/1272021_part_discount','.txt')
    # left_join_stdbaspdtdomtpd | 'WriteToText STDETAIL DTABLE DORDER MICROS TIME PART DISCOUNT' >> beam.io.WriteToText('data_output/03082021_1_left_stdbaspdtdomtpd','.txt')
    # cleaned_pay_payttpe | 'WriteToText PAID PAYTYPE' >> beam.io.WriteToText('data_output/2972021_1_paid_paytype','.txt')
    # left_join_ppstdbaspdtdomtpd | 'WriteToText PAID PAYTYPE STD DTDO MT PDISC' >> beam.io.WriteToText('data_output/2972021_1_ppstdbaspdtdomtpd','.txt')
    # stdetail_fact | 'WriteToText STDETAIL FACT' >> beam.io.WriteToText('data_output/02082021_1_stdetail_fact','.txt')

    # EXECUTE to BigQuery
    # branchessap_data
    # item_master_data
    # payment_type_data
    # clean_pay_payttpe 
    # left_join_stdbaspdtdomtpd

    # stdetail_fact | 'WriteToBigQuery STD ITEM DO DT' >> beam.io.WriteToBigQuery(
    #                             table_spec_std_fact,
    #                             schema=table_schema_stdetail_fact,
    #                             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #                             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    # left_join_stdbaspdtdomtpd | 'WriteToBigQuery STD ITEM DO DT' >> beam.io.WriteToBigQuery(
    #                             table_spec_std_fact,
    #                             schema=table_schema_stdetail_fact,
    #                             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #                             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    # Set the logger
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d:%H:%M:%S',
                        level=logging.INFO)
    # Run the core pipeline
    logging.info('Starting')
    run()