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


class CustomCoder(Coder):

    def encode(self, value):
        return value.encode("utf-8", "ignore")

    def decode(self, value):
        return value.decode("utf-8", "ignore")

    def is_deterministic(self):
        return True


class replaceprice(beam.DoFn):
    def process(self,data):
        try:
            if data['dorder_upref'] == '':
                    yield data
            else:
                if data['price'] == 0.0:
                    data['stdetail_net_price'] = data['price']
                yield data
        except:
            yield data

class mt_add_empty_data(beam.DoFn):
    def process(self,data):
        try:
            if data['dtype'] != '':
                yield data
            else: yield data
        except:
            data['dtype'] = None
            data['timetrans'] = None
            data['datetime_trans'] = None
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
            data['datetime_trans'] = None
            data['type_trans'] = None
            yield data

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
            data['dorder_ordertxt'] = None
            data['pdisc'] = None
            data['price'] = None
            data['dorder_upref'] = None
            data['dorder_receiveon'] = None
            # data['dorder_receiveon'] = None
            # data['dorder_receiveon_time'] = None

            yield data

class paid_std_add_data(beam.DoFn):
    def process(self, data):
        try:
            if data['paid_paid_amount'] != 0:
                yield data
            elif data['paid_paid_amount'] == 0: 
                if data['paid_types'] == None:
                    yield data   
                else: return             
        except:
            data['stdetail_seq'] = None
            data['plu'] = None
            data['stdetail_item_description'] = None
            data['stdetail_price'] = None
            data['stdetail_qty'] = None
            data['stdetail_amount'] = None
            data['stdetail_discount_percentage'] = None
            data['stdetail_discount_amount'] = None
            data['stdetail_extradisc_pct'] = None
            data['stdetail_extradisc_amt'] = None
            data['stdetail_net_price'] = None
            data['stdetail_point_recieved'] = None
            data['stdetail_flag_void'] = None
            data['stdetail_flag_status'] = None
            data['stdetail_upsizeto'] = None
            data['micros_transaction_number'] = None
            data['part_disc'] = None
            yield data

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
                # | 'print' >> beam.Map(Print())
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

#Remove Duplicate Data
def rmduplicate(data):
    seen = set()
    new_l = []
    for d in data:
        t = tuple(sorted(d.items()))
        if t not in seen:
            seen.add(t)
            new_l.append(d)
    return new_l

class BreakList(beam.DoFn):
    def process(self, data):
        for e in data:
            yield e

class clean_pay_paymenttype(beam.DoFn):
    def process(self, data):
        try:
            if data['paid_types'] != '':
                yield data
        except:
            data['paid_payment_description'] = None
            data['paid_types'] = None
            data['card_code'] = None
            yield data
    

# ==================================================

def discard_incomplete_transtypeMicros(data):
    """Filters out records that don't have an information."""
    return len(data['transaction_number']) > 0 and len(data['receiveOn']) != 'NULL'

# ====================CONVERT TYPE FUNCTIONS=============

def convert_types_branchessap(data):
    """Converts string values to their appropriate type."""

    data['branch_id'] = str(data['branch_id']) if 'branch_id' in data else None
    data['branch_sap'] = str(data['branch_sap']) if 'branch_sap' in data else None
    data['branch_name'] = str(data['branch_name']) if 'branch_name' in data else None
    data['branch_profile'] = int(data['branch_profile']) if 'branch_profile' in data else None
    return data

def convert_types_payment_types(data):
    """Converts string values to their appropriate type."""

    data['payment_types'] = int(data['payment_types']) if 'payment_types' in data else None
    data['paid_payment_description'] = str(data['paid_payment_description']) if 'paid_payment_description' in data else None
    data['paid_types'] = str(data['paid_types']) if 'paid_types' in data else None
    data['Seq'] = int(data['Seq']) if 'Seq' in data else None
    data['card_code'] = str(data['card_code']) if 'card_code' in data else None
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
    date_format2 = '%Y-%m-%d'
    datetrans = datetime.datetime.strptime(data['date'], date_format2)

    data['date'] = str(datetrans.date())
    time_trans = datetime.datetime.strptime(data['timetrans'],time_format)
    data['timetrans'] = str(time_trans.time())
    datetimestrans = datetime.datetime.combine(datetrans.date(), time_trans.time())
    data['datetime_trans'] = str(datetimestrans)
    
    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None
    data['type_trans'] = str(data['type_trans']) if 'type_trans' in data else None
    return data

def convert_types_partdics(data):
    """Converts string values to their appropriate type."""

    date_format3 = '%Y/%m/%d %H:%M:%S.%f000'
    time_format = '%H:%M:%S'
    data['card_no'] = str(data['card_no']) if 'card_no' in data else None
    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None
    disc_time = datetime.datetime.strptime(data['disc_time'], date_format3)
    data['disc_time'] = str(disc_time.date())
    data['plu'] = str(data['plu']) if 'plu' in data else None
    data['flag_void'] = int(data['flag_void']) if 'flag_void' in data else None
    copy_date = datetime.datetime.strptime(data['copy_date'], date_format3)
    data['copy_date'] = str(copy_date)
    data['copy_date_time'] = str(copy_date.time().strftime(time_format))
    data['branch_id'] = str(data['branch_id']) if 'branch_id' in data else None
    transaction_date = datetime.datetime.strptime(data['transaction_date'], date_format3)
    data['transaction_date'] = str(transaction_date.date())
    data['part_disc'] = bool(data['part_disc']) if 'part_disc' in data else None
    return data

def convert_types_paid(data):
    # Converts string values to their appropriate type.
    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None
    data['seq'] = int(data['seq']) if 'seq' in data else None
    data['payment_types'] = int(data['payment_types']) if 'payment_types' in data else None
    data['paid_paid_amount'] = float(data['paid_paid_amount']) if 'paid_paid_amount' in data else None
    return data

def convert_types_sales_transaction(data):
    # Converts string values to their appropriate type.

    date_format = '%Y/%m/%d %H:%M:%S.%f000'
    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None
    data['st_cashier_id'] = int(data['st_cashier_id']) if 'st_cashier_id' in data else None
    data['st_customer_id'] = str(data['st_customer_id']) if 'st_customer_id' in data else None
    data['st_spending_program_id'] = str(data['st_spending_program_id']) if 'st_spending_program_id' in data else None
    st_transaction_date = datetime.datetime.strptime(data['st_transaction_date'], date_format)
    data['st_transaction_date'] = str(st_transaction_date.date())
    data['year'] = str(st_transaction_date.year)
    data['month'] = str(st_transaction_date.month)
    data['day'] = str(st_transaction_date.day)
    data['dayname'] = str(st_transaction_date.strftime("%A"))
    data['week'] = str(st_transaction_date.strftime("%W"))
    data['st_total_discount'] = float(data['st_total_discount']) if 'st_total_discount' in data else None
    data['st_branch_id'] = str(data['st_branch_id']) if 'st_branch_id' in data else None
    data['st_cash_register_id'] = int(data['st_cash_register_id']) if 'st_cash_register_id' in data else None
    data['st_total_paid'] = float(data['st_total_paid']) if 'st_total_paid' in data else None
    data['st_net_price'] = float(data['st_net_price']) if 'st_net_price' in data else None
    data['st_tax'] = float(data['st_tax']) if 'st_tax' in data else None
    data['st_flag_return'] = int(data['st_flag_return']) if 'st_flag_return' in data else None
    data['st_register_return'] = int(data['st_register_return']) if 'st_register_return' in data else None
    st_trans_date_return = datetime.datetime.strptime(data['st_trans_date_return'], date_format) 
    data['st_trans_date_return'] = str(st_trans_date_return.date()) 
    data['st_trans_num_return'] = str(data['st_trans_num_return']) if 'st_trans_num_return' in data else None
    data['st_trans_time'] = str(data['st_trans_time']) if 'st_trans_time' in data else None
    data['st_store_type'] = str(data['st_store_type']) if 'st_store_type' in data else None
    st_receiveon = datetime.datetime.strptime(data['st_receiveon'], date_format)
    data['st_receiveon'] = str(st_receiveon.date()) 
    data['st_micros_transaction_number'] = str(data['st_micros_transaction_number']) if 'st_micros_transaction_number' in data else None
    return data

def convert_types_sales_transaction_details(data):
    """Converts string values to their appropriate type."""

    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None
    data['stdetail_seq'] = int(data['stdetail_seq']) if 'stdetail_seq' in data else None
    data['plu'] = str(data['plu']) if 'plu' in data else None
    data['stdetail_item_description'] = str(data['stdetail_item_description']) if 'stdetail_item_description' in data else None
    data['stdetail_price'] = float(data['stdetail_price']) if 'stdetail_price' in data else None

    # data['stdetail_qty'] = int(data['stdetail_qty']) if 'stdetail_qty' in data else None #ada nilai negatif (-1) gabisa di convert ke int
    data['stdetail_qty'] = float(data['stdetail_qty']) if 'stdetail_qty' in data else None #ada nilai negatif (-1) gabisa di convert ke int

    data['stdetail_amount'] = float(data['stdetail_amount']) if 'stdetail_amount' in data else None
    data['stdetail_discount_percentage'] = float(data['stdetail_discount_percentage']) if 'stdetail_discount_percentage' in data else None
    data['stdetail_discount_amount'] = float(data['stdetail_discount_amount']) if 'stdetail_discount_amount' in data else None
    data['stdetail_extradisc_pct'] = float(data['stdetail_extradisc_pct']) if 'stdetail_extradisc_pct' in data else None
    data['stdetail_extradisc_amt'] = float(data['stdetail_extradisc_amt']) if 'stdetail_extradisc_amt' in data else None
    data['stdetail_net_price'] = float(data['stdetail_net_price']) if 'stdetail_net_price' in data else None
    data['stdetail_point_received'] = int(data['stdetail_point_received']) if 'stdetail_point_received' in data else None
    data['stdetail_flag_void'] = int(data['stdetail_flag_void']) if 'stdetail_flag_void' in data else None
    data['stdetail_flag_status'] = int(data['stdetail_flag_status']) if 'stdetail_flag_status' in data else None
    data['stdetail_flag_paket_discount'] = int(data['stdetail_flag_paket_discount']) if 'stdetail_flag_paket_discount' in data else None
    data['stdetail_upsizeto'] = str(data['stdetail_upsizeto']) if 'stdetail_upsizeto' in data else None
    data['micros_transaction_number'] = str(data['micros_transaction_number']) if 'micros_transaction_number' in data else None
    data['sbu'] = int(data['sbu']) if 'sbu' in data else None
    return data

def convert_types_daily_table(data):
    """Converts string values to their appropriate type."""
    
    date_format = '%Y/%m/%d %H:%M:%S.%f000'
    # data['sales_date'] = str(data['sales_date']) if 'sales_date' in data else None
    sales_date = datetime.datetime.strptime(data['sales_date'], date_format)
    data['sales_date'] = str(sales_date.date())
    data['cash_register_id'] = str(data['cash_register_id']) if 'cash_register_id' in data else None
    data['order_id'] = int(data['order_id']) if 'order_id' in data else None
    data['branch_id'] = str(data['branch_id']) if 'branch_id' in data else None
    # data['dtable_receiveOn'] = str(data['dtable_receiveOn']) if 'dtable_receiveOn' in data else None
    # dtable_receiveOn_date = datetime.datetime.strptime(data['dtable_receiveOn_date'], date_format)
    # data['dtable_receiveOn_date'] = str(dtable_receiveOn_date.date())
    data['transaction_number'] = str(data['transaction_number']) if 'transaction_number' in data else None
    # data['micros_transaction_number'] = str(data['micros_transaction_number']) if 'micros_transaction_number' in data else None
    return data

def convert_types_daily_order(data):
    """Converts string values to their appropriate type."""

    date_format = '%Y/%m/%d %H:%M:%S.%f000'
    time_format = '%H:%M:%S'

    # data['sales_date'] = str(data['sales_date']) if 'sales_date' in data else None
    sales_date = datetime.datetime.strptime(data['sales_date'], date_format)
    data['sales_date'] = str(sales_date.date())
    # data['sales_date_year'] = str(sales_date.year)
    # data['sales_date_month'] = str(sales_date.month)
    # data['sales_date_day'] = str(sales_date.day)
    # data['sales_date_dayname'] = str(sales_date.strftime("%A"))
    # data['sales_date_weeks'] = str(sales_date.strftime("%W"))
    data['cash_register_id'] = str(data['cash_register_id']) if 'cash_register_id' in data else None
    data['order_id'] = int(data['order_id']) if 'order_id' in data else None
    # data['order_idx'] = int(data['order_idx']) if 'order_idx' in data else None
    data['plu'] = str(data['plu']) if 'plu' in data else None
    data['dorder_ordertxt'] = str(data['dorder_ordertxt']) if 'dorder_ordertxt' in data else None
    data['pdisc'] = int(data['pdisc']) if 'pdisc' in data else None
    data['price'] = float(data['price']) if 'price' in data else None
    data['branch_id'] = str(data['branch_id']) if 'branch_id' in data else None
    # data['dorder_receiveon'] = str(data['dorder_receiveon']) if 'dorder_receiveon' in data else None
    dorder_receiveon = datetime.datetime.strptime(data['dorder_receiveon'], date_format)
    data['dorder_receiveon'] = str(dorder_receiveon.date())
    # data['dorder_receiveon_time'] = str(dorder_receiveon.time().strftime(time_format))
    data['dorder_upref'] = str(data['dorder_upref']) if 'dorder_upref' in data else None
    
    data['order_idx'] = int(data['order_idx']) if 'order_idx' in data else None
    data['qty'] = float(data['qty']) if 'qty' in data else None
    # data['qty'] = int(data['qty']) if 'qty' in data else None
    return data

def convert_types_item_master(data):
    """Converts string values to their appropriate type."""
    data['plu'] = str(data['plu']) if 'plu' in data else None
    data['item_burui'] = str(data['item_burui']) if 'item_burui' in data else None
    # data['item_description'] = str(data['item_description']) if 'item_description' in data else None
    data['item_article_code'] = str(data['item_article_code']) if 'item_article_code' in data else None
    data['item_long_description'] = str(data['item_long_description']) if 'item_long_description' in data else None
    return data


# ============== DELETE COLUMNS FUNCTIONS=================


def del_unwanted_cols_transtypemmicros(data):
    """Delete the unwanted columns"""
    del data['receiveOn']
    del data['sales_date']
    return data

def del_unwanted_cols_transtime(data):
    """Delete the unwanted columns"""
    del data['date']
    return data

def del_unwanted_cols_payment_types(data):
    """Delete the unwanted columns"""
    del data['Seq']
    return data    

def del_unwanted_cols_partdisc(data):
    """Delete the unwanted columns"""
    del data['card_no']
    return data 

def del_unwanted_cols_paid(data):
    """Delete the unwanted columns"""
    del data['seq']
    del data['Currency_ID']
    del data['Currency_Rate']
    del data['Credit_Card_No']
    del data['Credit_Card_Name']
    del data['Shift']
    del data['micros_transaction_number']
    return data  

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
    del data['st_micros_transaction_number']
    return data

# def del_unwanted_cols_sales_transaction_details(data):
#     """Delete the unwanted columns"""
#     del data['Seq']
#     del data['Price']
#     del data['Points_Received']
#     del data['Flag_Status']
#     del data['Flag_Paket_Discount']
#     del data['upsizeTo']
#     return data

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
    del data['mn_id']
    del data['mn_idx']
    del data['sz_id']
    del data['k_id']
    # del data['qty']
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
    del data['order_idx']
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

def del_unwanted_st_bsap_for_dimm(data):
    del data['st_cashier_id']
    del data['st_customer_id']
    del data['st_spending_program_id']
    del data['st_total_discount']
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
    del data['st_micros_transaction_number']
    return data

def del_unwanted_cols_st_branchessap(data):
    """Delete the unwanted columns"""
    del data['st_cashier_id']
    del data['st_customer_id']
    del data['st_spending_program_id']
    del data['st_transaction_date']
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
    # del data['micros_transaction_number']
    # del data['st_micros_transaction_number']
    del data['branch_sap']
    del data['branch_name']
    del data['branch_profile']
    return data


def dodt_removeprice(data):
    """Delete the unwanted columns"""
    del data['price']
    return data

def rmodt2(data):
    """Delete the unwanted columns"""
    del data['sales_date']
    del data['cash_register_id']
    del data['order_id']
    del data['branch_id']
    del data['micros_transaction_number']
    del data['dorder_ordertxt']
    del data['pdisc']
    del data['price']
    del data['dorder_receiveon']
    return data



# def del_columns(data):
#     del data['st_cashier_id']
#     del data['st_customer_id']
#     del data['st_spending_program_id']
#     del data['st_transaction_date']
#     del data['st_total_discount']
#     del data['st_branch_id']
#     del data['st_cash_register_id']
#     del data['st_total_paid']
#     del data['st_net_price']
#     del data['st_tax']
#     del data['st_flag_return']
#     del data['st_register_return']
#     del data['st_trans_date_return']
#     del data['st_trans_num_return']
#     del data['st_trans_time']
#     del data['st_store_type']
#     del data['st_receiveon']
#     del data['branch_sap']
#     del data['branch_name']
#     del data['branch_profile']
    
#     return data

def del_unwanted_cols_fact(data):
    """Delete the unwanted columns"""
    # del data['do_plu']
    del data['dorder_receiveon']
    # del data['dorder_receiveon_time']
    return data

def del_paid_amount(data):
    """Delete the unwanted columns"""
    # del data['do_plu']
    del data['paid_paid_amount']
    # del data['dorder_receiveon_time']
    return data

def del_qty(data):
    """Delete the unwanted columns"""
    del data['qty']
    return data

def delColDodtFact(data):
    """Delete the unwanted columns"""
    del data['qty']
    # del data['stdetail_seq'] #klo left join, ini gabisa dihapus
    # del data['stdetail_item_description'] #klo left join, ini gabisa dihapus
    # del data['stdetail_price']
    del data['stdetail_qty']
    # del data['stdetail_amount'] #klo left join, ini gabisa dihapus
    # del data['stdetail_discount_percentage']
    # del data['stdetail_discount_amount']
    # del data['stdetail_extradisc_pct']
    # del data['stdetail_extradisc_amt']
    # del data['stdetail_point_received']
    # del data['stdetail_flag_void']
    # del data['stdetail_flag_status']
    # del data['stdetail_flag_paket_discount']
    # del data['stdetail_upsizeto']
    # del data['part_disc']
    return data

def delColstd(data):
    """Delete the unwanted columns"""
    # del data['qty']
    del data['stdetail_seq'] #klo left join, ini gabisa dihapus
    del data['stdetail_item_description'] #klo left join, ini gabisa dihapus
    del data['stdetail_price']
    # del data['stdetail_qty']
    del data['stdetail_amount'] #klo left join, ini gabisa dihapus
    del data['stdetail_discount_percentage']
    del data['stdetail_discount_amount']
    del data['stdetail_extradisc_pct']
    del data['stdetail_extradisc_amt']
    del data['stdetail_point_received']
    del data['stdetail_flag_void']
    del data['stdetail_flag_status']
    del data['stdetail_flag_paket_discount']
    del data['stdetail_upsizeto']
    del data['part_disc']
    return data


# def convert_none_island4(data):
#     """Converts string values '' to None."""
#     # data['transaction_number'] == None if len(data['transaction_number']) > 0 in data else data['transaction_number']
#     # data['st_customer_id'] == None 
#     for k in data:
#         if data[k] == '':
#             data[k] == None
#     yield data

# ================JOIN FUNCTIONS=================
# ===============================================


schema_partdisc = (
    'card_no:STRING,\
    transaction_number:STRING,\
    disc_time:STRING,\
    disc_time_date:DATE,\
    plu:STRING,\
    flag_void:INTEGER,\
    copy_date:STRING,\
    copy_date_date:DATE,\
    branch_id:INTEGER,\
    transaction_date:STRING,\
    transaction_date_date:DATE')


# ==========================================

schema_st_fact=(
    'transaction_number:STRING,\
    st_cashier_id:INTEGER,\
    st_customer_id:STRING,\
    st_spending_program_id:STRING,\
    st_transaction_date:DATE,\
    st_transaction_year:STRING,\
    st_transaction_month:STRING,\
    st_transaction_day:STRING,\
    st_transaction_dayname:STRING,\
    st_transaction_week:STRING,\
    st_total_discount:FLOAT,\
    st_branch_id:INTEGER,\
    st_cash_register_id:INTEGER,\
    st_total_paid:FLOAT,\
    st_net_price:FLOAT,\
    st_tax:FLOAT,\
    st_flag_return:INTEGER,\
    st_register_return:INTEGER,\
    st_trans_date_return:DATE,\
    st_trans_num_return:STRING,\
    st_trans_time:STRING,\
    st_store_type:STRING,\
    st_receiveon:DATE'
    )

#SCHEMA PAID >< PAYMENT >< STBSAP >< DTDO >< MICROS
schema_paid_fact=(
    'transaction_number:STRING,\
    payment_types:INTEGER,\
    paid_paid_amount:FLOAT,\
    paid_payment_description:STRING,\
    paid_types:STRING,\
    card_code:STRING,\
    sales_date:DATE,\
    cash_register_id:STRING,\
    order_id:INTEGER,\
    branch_id:STRING,\
    micros_transaction_number:STRING,\
    plu:STRING,\
    dorder_ordertxt:STRING,\
    pdisc:INTEGER,\
    price:FLOAT,\
    dorder_upref:STRING,\
    dorder_receiveon:DATE,\
    dtype:STRING,\
    timetrans:TIME,\
    datetime_trans:DATETIME,\
    type_trans:STRING'
)

#SCHEMA PAID FACT >< ITEM
schema_paid_item=(
    'transaction_number:STRING,\
    payment_types:INTEGER,\
    paid_paid_amount:FLOAT,\
    paid_payment_description:STRING,\
    paid_types:STRING,\
    card_code:STRING,\
    st_transaction_year:STRING,\
    st_transaction_month:STRING,\
    st_transaction_day:STRING,\
    st_transaction_dayname:STRING,\
    st_transaction_week:STRING,\
    branch_id:STRING,\
    sales_date:DATE,\
    cash_register_id:STRING,\
    order_id:INTEGER,\
    micros_transaction_number:STRING,\
    plu:STRING,\
    dorder_ordertxt:STRING,\
    pdisc:INTEGER,\
    price:FLOAT,\
    dorder_upref:STRING,\
    dorder_receiveon:DATE,\
    dtype:STRING,\
    timetrans:TIME,\
    datetime_trans:DATETIME,\
    type_trans:STRING,\
    item_plu:STRING,\
    item_burui:STRING,\
    item_article_code:STRING,\
    item_long_description:STRING'
)

# SCHEMA DETAILS FULL
# schema_stdetails_fact = (
#     'transaction_number:STRING,\
#     stdetail_seq:INTEGER,\
#     plu:STRING,\
#     stdetail_item_description:STRING,\
#     stdetail_price:FLOAT,\
#     stdetail_qty:INTEGER,\
#     stdetail_amount:FLOAT,\
#     stdetail_discount_percentage:FLOAT,\
#     stdetail_discount_amount:FLOAT,\
#     stdetail_extradisc_pct:FLOAT,\
#     stdetail_extradisc_amt:FLOAT,\
#     stdetail_net_price:FLOAT,\
#     stdetail_point_received:INTEGER,\
#     stdetail_flag_void:INTEGER,\
#     stdetail_flag_status:INTEGER,\
#     stdetail_flag_paket_discount:INTEGER,\
#     stdetail_upsizeto:STRING,\
#     micros_transaction_number:STRING,\
#     part_disc:BOOLEAN,\
#     st_transaction_date:DATE,\
#     st_year:STRING,\
#     st_month:STRING,\
#     st_day:STRING,\
#     st_dayname:STRING,\
#     st_week:STRING,\
#     branch_id:STRING,\
#     sales_date:DATE,\
#     cash_register_id:STRING,\
#     order_id:INTEGER,\
#     qty:INTEGER,\
#     dorder_ordertxt:STRING,\
#     pdisc:INTEGER,\
#     price:FLOAT,\
#     dorder_upref:STRING,\
#     dorder_receiveon:DATE,\
#     dtype:STRING,\
#     timetrans:TIME,\
#     datetime_trans:DATETIME,\
#     type_trans:STRING,\
#     disc_time:DATE,\
#     flag_void:INTEGER,\
#     copy_date:DATETIME,\
#     copy_date_time:TIME,\
#     transaction_date:DATE'
#     )

# # New Schema STD FACT
# schema_stdetails_fact = (
#     'transaction_number:STRING,\
#     stdetail_seq:INTEGER,\
#     plu:STRING,\
#     stdetail_item_description:STRING,\
#     stdetail_price:FLOAT,\
#     stdetail_qty:INTEGER,\
#     stdetail_amount:FLOAT,\
#     stdetail_discount_percentage:FLOAT,\
#     stdetail_discount_amount:FLOAT,\
#     stdetail_extradisc_pct:FLOAT,\
#     stdetail_extradisc_amt:FLOAT,\
#     stdetail_net_price:FLOAT,\
#     stdetail_point_received:INTEGER,\
#     stdetail_flag_void:INTEGER,\
#     stdetail_flag_status:INTEGER,\
#     stdetail_flag_paket_discount:INTEGER,\
#     stdetail_upsizeto:STRING,\
#     micros_transaction_number:STRING,\
#     part_disc:BOOLEAN,\
#     sbu:INTEGER,\
#     st_transaction_date:DATE,\
#     year:STRING,\
#     month:STRING,\
#     day:STRING,\
#     dayname:STRING,\
#     week:STRING,\
#     branch_id:STRING,\
#     dtype:STRING,\
#     timetrans:TIME,\
#     datetime_trans:DATETIME,\
#     type_trans:STRING,\
#     disc_time:DATE,\
#     flag_void:INTEGER,\
#     copy_date:DATETIME,\
#     copy_date_time:TIME,\
#     transaction_date:DATE'
#     )

# New Schema STD FACT
schema_dodt_fact = (
    'sales_date:DATE,\
    cash_register_id:STRING,\
    order_id:INTEGER,\
    plu:STRING,\
    qty:INTEGER,\
    dorder_ordertxt:STRING,\
    pdisc:INTEGER,\
    price:FLOAT,\
    dorder_upref:STRING,\
    branch_id:STRING,\
    dorder_receiveon:DATE,\
    transaction_number:STRING,\
    micros_transaction_number:STRING,\
    stdetail_qty:INTEGER,\
    stdetail_net_price:FLOAT,\
    sbu:INTEGER,\
    year:STRING,\
    month:STRING,\
    day:STRING,\
    dayname:STRING,\
    week:STRING'
    )

# schema_stdetails_fact = (
#     'transaction_number:STRING,\
#     stdetail_seq:INTEGER,\
#     plu:STRING,\
#     stdetail_item_description:STRING,\
#     stdetail_price:FLOAT,\
#     stdetail_qty:INTEGER,\
#     stdetail_amount:FLOAT,\
#     stdetail_discount_percentage:FLOAT,\
#     stdetail_discount_amount:FLOAT,\
#     stdetail_extradisc_pct:FLOAT,\
#     stdetail_extradisc_amt:FLOAT,\
#     stdetail_net_price:FLOAT,\
#     stdetail_point_received:INTEGER,\
#     stdetail_flag_void:INTEGER,\
#     stdetail_flag_status:INTEGER,\
#     stdetail_flag_paket_discount:INTEGER,\
#     stdetail_upsizeto:STRING,\
#     micros_transaction_number:STRING,\
#     part_disc:BOOLEAN,\
#     sbu:INTEGER,\
#     year:STRING,\
#     month:STRING,\
#     day:STRING,\
#     dayname:STRING,\
#     week:STRING,\
#     branch_id:STRING,\
#     dtype:STRING,\
#     timetrans:TIME,\
#     datetime_trans:DATETIME,\
#     type_trans:STRING,\

    
    
#     sales_date:DATE,\
#     cash_register_id:STRING,\
#     order_id:INTEGER,\
#     order_idx:INTEGER,\
#     qty:INTEGER,\
#     dorder_ordertxt:STRING,\
#     pdisc:INTEGER,\
#     price:FLOAT,\
#     dorder_upref:STRING,\
#     dorder_receiveon:DATE,\
#     disc_time:DATE,\
#     flag_void:INTEGER,\
#     copy_date:DATETIME,\
#     copy_date_time:TIME,\
#     transaction_date:DATE'
#     )

# tb_spec='wired-glider-289003:lake.sb-test-paid'
project_id = 'wired-glider-289003'  # replace with your project ID
dataset_id = 'starbuck_data_samples'  # replace with your dataset ID

# project_id = 'ps-id-starbucks-da-05052021'  # replace with your project ID
# dataset_id = 'stbck_sales'  # replace with your dataset ID

table_id_dodt = 'SB_DODT_FACT_1M'  # replace with your table ID

# parameters
job_name = "dodt-fact-1m" # replace with your job name
temp_location=f'gs://{project_id}/temp'
staging_location = f'gs://{project_id}/starbucks-BOH/staging' # replace with  your folder destination
max_num_workers=10 # replace with preferred num_workers
worker_region='asia-southeast1' #replace with your worker region

def run(argv=None):

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    options = PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        job_name=job_name,
        temp_location=temp_location,
        region=worker_region,
        machine_type='n1-standard-8',
        autoscaling_algorithm='THROUGHPUT_BASED',
        max_num_workers=max_num_workers,
        # disk_size_gb=500,
        save_main_session = True
    )

    p = beam.Pipeline(options=PipelineOptions())
    # p = beam.Pipeline(options=options) #dataflow

    branchessap_data = (p 
                        | 'ReadData branchessap type' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_BranchesSap_20210607_093034.txt', skip_header_lines =1)
                        # | 'ReadData branchessap type' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_BranchesSap_20210710_065124.txt', skip_header_lines =1)
                        # | 'ReadData branchessap type' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_BranchesSap_20210721_190925.txt', skip_header_lines =1)
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/BranchesSap_20210607_093034.txt', skip_header_lines =1)
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_BranchesSap_20210710_065124.txt', skip_header_lines =1)
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_BranchesSap_20210721_190925.txt', skip_header_lines =1)
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-BranchesSap_20210801_211616.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-BranchesSap_20210801_213849.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-BranchesSap_20210801_220059.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/BranchesSap_20210728_140524.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://stbck_sales_data/3D_data_source_20210501/3d_data_source_BranchesSap_20210710_065124.txt', skip_header_lines =1)
                        | 'SplitData Branches_sap' >> beam.Map(lambda x: x.split('|')) #hapus spasi pakai python strip
                        | 'FormatToDict Branches_sap' >> beam.Map(lambda x: {
                            "branch_id": x[0].strip(),
                            "branch_sap": x[1].strip(),
                            "branch_name": x[2].strip(),
                            "branch_profile": x[3].strip()
                            }) 
                        # | 'DeleteIncompleteData Branches_sap' >> beam.Filter(discard_incomplete_branchessap)
                        | 'ChangeDataType Branches_sap' >> beam.Map(convert_types_branchessap)
                        # | 'DeleteUnwantedData Branches_sap' >> beam.Map(del_unwanted_cols_branchessap)
                        # | 'Write Branches_sap' >> WriteToText('data_source_BOH/output/data-branchessap','.txt')
                        )

    daily_table_data = (p 
                        | 'ReadData daily table' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_daily_table_20210607_094807.txt', skip_header_lines =1)
                        # | 'ReadData daily table' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_daily_table_20210710_065220.txt', skip_header_lines =1)
                        # | 'ReadData daily table' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_daily_table_20210721_190936.txt', skip_header_lines =1)
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/daily_table_20210607_094807.txt', skip_header_lines =1)
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_daily_table_20210710_065220.txt', skip_header_lines =1)
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_daily_table_20210721_190936.txt', skip_header_lines =1)
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-daily_table_20210801_211053.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-daily_table_20210801_212940.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-daily_table_20210801_215201.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/daily_table_20210728_140540.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://stbck_sales_data/3D_data_source_20210501/3d_data_source_daily_table_20210710_065220.txt', skip_header_lines =1)
                        | 'SplitData daily table' >> beam.Map(lambda x: x.split('|'))
                        | 'FormatToDict daily table' >> beam.Map(lambda x: {
                            "sales_date": x[0],
                            "cash_register_id": x[1],
                            "order_id": x[2],
                            "numtable": x[3], #hapus
                            "status": x[4], #hapus
                            "dtype": x[5], #hapus
                            "transaction_number": x[6],
                            "bill_seq": x[7], #hapus
                            "cover": x[8], #hapus
                            "branch_id": x[9],
                            "receiveOn": x[10],
                            "micros_transaction_number": x[11]
                        }) 
                        # | 'DeleteIncompleteData transaction' >> beam.Filter(discard_incomplete_sales_transaction)
                        | 'ChangeDataType daily table' >> beam.Map(convert_types_daily_table)
                        | 'DeleteUnwantedData daily table' >> beam.Map(del_unwanted_cols_daily_table)
                        # | 'format data transaction' >> beam.Map(changenulltonodata)
                        # | 'Write daily table' >> WriteToText('data_source_BOH/output/060821-daily_table','.txt')
                        )

    daily_order_data = (p 
                        | 'ReadData daily order' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_daily_order_20210607_094920.txt', skip_header_lines =1)
                        # | 'ReadData daily order' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_daily_order_20210710_065323.txt', skip_header_lines =1)
                        # | 'ReadData daily order' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_daily_order_20210721_191008.txt', skip_header_lines =1)
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/daily_order_20210607_094920.txt', skip_header_lines =1)
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_daily_order_20210710_065323.txt', skip_header_lines =1)
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_daily_order_20210721_191008.txt', skip_header_lines =1)
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-daily_order_20210801_211102.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-daily_order_20210801_212949.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-daily_order_20210801_215211.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/daily_order_20210728_140624.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://stbck_sales_data/3D_data_source_20210501/3d_data_source_daily_order_20210710_065323.txt', skip_header_lines =1)
                        | 'SplitData daily order' >> beam.Map(lambda x: x.split('|'))
                        | 'FormatToDict daily order' >> beam.Map(lambda x: {
                            "sales_date": x[0],
                            "cash_register_id": x[1],
                            "order_id": x[2],
                            "order_idx": x[3],
                            "mn_id": x[4], #hapus
                            "mn_idx": x[5], #hapus
                            "sz_id": x[6], #hapus
                            "k_id": x[7], #hapus
                            "plu": x[8].strip(),
                            "qty": x[9], #hapus
                            "flag_void": x[10], #hapus
                            "status": x[11], #hapus
                            "printed": x[12], #hapus
                            "dorder_ordertxt": x[13],
                            "userid": x[14], #hapus
                            "pdisc": x[15], #hapus
                            "pdisc_no": x[16], #hapus
                            "upsize": x[17], #hapus
                            "upsizeto": x[18], #hapus
                            "price": x[19], #hapus
                            "Fp": x[20], #hapus
                            "dorder_upref": x[21].strip(),
                            "branch_id": x[22],
                            "dorder_receiveon": x[23]
                            }) 
                        # | 'DeleteIncompleteData transaction' >> beam.Filter(discard_incomplete_sales_transaction)
                        | 'ChangeDataType daily order' >> beam.Map(convert_types_daily_order)
                        | 'DeleteUnwantedData daily order' >> beam.Map(del_unwanted_cols_daily_order)
                        # | 'format data transaction' >> beam.Map(changenulltonodata)
                        # | 'Write daily order' >> WriteToText('output/daily_order','.txt')
                        )

    # rm_duplicate_do = (daily_order_data  | 'Convert DO Dict to String' >> beam.Map(lambda x: str(x))
    #                                 | 'Remove Duplicate DO' >> beam.Distinct()
    #                                 | 'Convert String to DO' >> beam.Map(lambda x : eval(x))
    #                                 # | 'Write daily order' >> WriteToText('data_source_BOH/output/060821-rm-daily_order','.txt')
    # ) 

    partdics_data = (p 
                        | 'ReadData partdics ' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_part_disc_20210607_095311.txt', skip_header_lines =1)
                        # | 'ReadData partdics' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_part_disc_20210710_064602.txt', skip_header_lines =1)
                        # | 'ReadData partdics' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_part_disc_20210721_185935.txt', skip_header_lines =1)
                        # | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/part_disc_20210607_095311.txt', skip_header_lines =1)
                        # | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_part_disc_20210710_064602.txt', skip_header_lines =1)
                        # | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_part_disc_20210721_185935.txt', skip_header_lines =1)
                        # | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-part_disc_20210801_210649.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-part_disc_20210801_212643.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-part_disc_20210801_214935.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/part_disc_20210728_140151.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData partdics ' >> beam.io.ReadFromText('gs://stbck_sales_data/3D_data_source_20210501/3d_data_source_part_disc_20210710_064602.txt', skip_header_lines =1)
                        | 'SplitData partdisc' >> beam.Map(lambda x: x.split('|'))                    
                        | 'FormatToDict partdisc' >> beam.Map(lambda x: {
                            "card_no": x[0].strip(),
                            "transaction_number": x[1].strip(),
                            "disc_time": x[2].strip(),
                            "plu": x[3].strip(),
                            "flag_void": x[4].strip(),
                            "copy_date": x[5].strip(),
                            "copy_date_time": None,
                            "branch_id": x[6].strip(),
                            "transaction_date": x[7].strip(),
                            "part_disc": True
                            }) 
                        # | 'DeleteIncompleteData partdisc' >> beam.Filter(discard_incomplete_partdics)
                        | 'ChangeDataType partdisc' >> beam.Map(convert_types_partdics)
                        | 'DeleteUnwantedData partdisc' >> beam.Map(del_unwanted_cols_partdisc)
                        # | 'Write partdisc' >> WriteToText('data_source_BOH/output/data-partdisc','.txt')
                        )

    sales_transaction_data = (p 
                        | 'ReadData transaction' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_sales_transactions_20210607_094416.txt', skip_header_lines =1)
                        # | 'ReadData transaction' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_sales_transactions_20210709_102056.txt', skip_header_lines =1)
                        # | 'ReadData transaction' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_sales_transactions_20210721_184841.txt', skip_header_lines =1)
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/sales_transactions_20210607_094416.txt', skip_header_lines =1)
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_sales_transactions_20210709_102056.txt', skip_header_lines =1)
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_sales_transactions_20210721_184841.txt', skip_header_lines =1)
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-sales_transactions_20210801_210110.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-sales_transactions_20210801_212107.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-sales_transactions_20210801_214416.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/sales_transactions_20210728_132936.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://stbck_sales_data/3D_data_source_20210501/3d_data_source_sales_transactions_20210709_102056.txt', skip_header_lines =1)
                        | 'SplitData transaction' >> beam.Map(lambda x: x.split('|'))
                        | 'FormatToDict transaction' >> beam.Map(lambda x: {
                            "transaction_number": x[0],
                            "st_cashier_id": x[1],
                            "st_customer_id": x[2].strip(),
                            "Card_Number": x[3], #delete
                            "st_spending_program_id": x[4].strip(),
                            "st_transaction_date": x[5],
                            "year": None,
                            "month": None,
                            "day": None,
                            "dayname": None,
                            "week": None,
                            "st_total_discount": x[6],
                            "Points_Of_Spending_Program": x[7], #delete
                            "Point_Of_Item_Program": x[8], #delete
                            "Point_Of_Card_Program": x[9], #delete
                            "Payment_Program_ID": x[10], #delete
                            "st_branch_id": x[11],
                            "st_cash_register_id": x[12],
                            "st_total_paid": x[13],
                            "st_net_price": x[14],
                            "st_tax": x[15],
                            "Net_Amount": x[16], #delete
                            "Change_Amount": x[17], #delete
                            "Flag_Arrange": x[18], #delete
                            "WorkManShip": x[19], #delete
                            "st_flag_return": x[20],
                            "st_register_return": x[21],
                            "st_trans_date_return": x[22],
                            "st_trans_num_return": x[23].strip(),
                            "Last_Point": x[24], #delete
                            "Get_Point": x[25], #delete
                            "Status": x[26], #delete
                            "Upload_Status": x[27], #delete
                            "st_trans_time": x[28].strip(),
                            "st_store_type": x[29].strip(),
                            "st_receiveon": x[30],
                            "st_micros_transaction_number": x[31]
                            }) 
                        # | 'DeleteIncompleteData transaction' >> beam.Filter(discard_incomplete_sales_transaction)
                        | 'ChangeDataType transaction' >> beam.Map(convert_types_sales_transaction)
                        | 'DeleteUnwantedData transaction' >> beam.Map(del_unwanted_cols_sales_transaction)
                        # | 'format data transaction' >> beam.Map(changenulltonodata)
                        # | 'Write sales transaction' >> WriteToText('data_source_BOH/output/data-sales_transaction','.txt')
                        )

    sales_transaction_details_data = (p 
                        | 'ReadData transaction details' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_sales_transaction_details_20210607_094608.txt', skip_header_lines =1)
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_sales_transaction_details_20210709_135236.txt', skip_header_lines =1)
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_sales_transaction_details_20210721_185124.txt', skip_header_lines =1)
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/sales_transaction_details_20210607_094608.txt', skip_header_lines =1)
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_sales_transaction_details_20210709_135236.txt', skip_header_lines =1)
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_sales_transaction_details_20210721_185124.txt', skip_header_lines =1)
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-sales_transaction_details_20210801_210422.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-sales_transaction_details_20210801_212400.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-sales_transaction_details_20210801_214652.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/sales_transaction_details_20210728_133857.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://stbck_sales_data/3D_data_source_20210501/3d_data_source_sales_transaction_details_20210709_135236.txt', skip_header_lines =1)
                        | 'SplitData transaction details' >> beam.Map(lambda x: x.split('|'))
                        | 'FormatToDict transaction details' >> beam.Map(lambda x: {
                            "transaction_number": x[0],
                            "stdetail_seq": x[1],
                            "plu": x[2].strip(),
                            "stdetail_item_description": x[3],
                            "stdetail_price": x[4],
                            "stdetail_qty": x[5],
                            "stdetail_amount": x[6],
                            "stdetail_discount_percentage": x[7],
                            "stdetail_discount_amount": x[8],
                            "stdetail_extradisc_pct": x[9],
                            "stdetail_extradisc_amt": x[10],
                            "stdetail_net_price": x[11],
                            "stdetail_point_received": x[12],
                            "stdetail_flag_void": x[13],
                            "stdetail_flag_status": x[14],
                            "stdetail_flag_paket_discount": x[15],
                            "stdetail_upsizeto": x[16],
                            "micros_transaction_number": x[17],
                            "part_disc": None,
                            "sbu":1
                            }) 
                        # | 'DeleteIncompleteData transaction' >> beam.Filter(discard_incomplete_sales_transaction)
                        | 'ChangeDataType transaction details' >> beam.Map(convert_types_sales_transaction_details)
                        # | 'DeleteUnwantedData transaction details' >> beam.Map(delColstd)
                        # | 'format data transaction' >> beam.Map(changenulltonodata)
                        # | 'Write sales transaction details' >> WriteToText('data_source_BOH/output/data-sales_transaction_details','.txt')
                        )

    transtypemicros_data = (p 
                        | 'ReadData transtype micros' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_Transaction_Type_Micros_20210611_124914.txt', skip_header_lines =1)
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_Transaction_Type_Micros_20210716_134914.txt', skip_header_lines =1)
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_Transaction_Type_Micros_20210721_193526.txt', skip_header_lines =1)
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/Transaction_Type_Micros_20210611_124914.txt', skip_header_lines =1)
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_Transaction_Type_Micros_20210716_134914.txt', skip_header_lines =1)
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_Transaction_Type_Micros_20210721_193526.txt', skip_header_lines =1)
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-Transaction_Type_Micros_202108021.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-Transaction_Type_Micros_202108023.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-Transaction_Type_Micros_202108022.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/Transaction_Type_Micros_20210730_090844.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://stbck_sales_data/3D_data_source_20210501/3d_data_source_Transaction_Type_Micros_20210716_134914.txt', skip_header_lines =1)
                        | 'SplitData transtype micros' >> beam.Map(lambda x: x.split('|'))
                        | 'FormatToDict transtype micros' >> beam.Map(lambda x: {
                            "transaction_number": x[0].strip(),
                            "dtype": x[1].strip(),
                            "receiveOn": x[2].strip(),
                            "sales_date": x[3].strip()
                            }) 
                        | 'DeleteIncompleteData transtype micros' >> beam.Filter(discard_incomplete_transtypeMicros)
                        | 'ChangeDataType transtype micros' >> beam.Map(convert_types_transtypemicros)
                        | 'DeleteUnwantedData transtype micros' >> beam.Map(del_unwanted_cols_transtypemmicros)
                        # | 'Write transtype micros' >> WriteToText('output/transtype_micros','.txt')
                        )

                        
    transtypetime_data = (p 
                        | 'ReadData transtype tipe' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_Transaction_Type_Time_20210607_102017.txt', skip_header_lines =1)
                        # | 'ReadData transtype tipe' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_Transaction_Type_Time__20210716_155124.txt', skip_header_lines =1)
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_Transaction_Type_Time_20210721_192925.txt', skip_header_lines =1)
                        # | 'ReadData transtype tipe' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/Transaction_Type_Time_20210611_124917.txt', skip_header_lines =1)
                        # | 'ReadData transtype tipe' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_Transaction_Type_Time__20210716_155124.txt', skip_header_lines =1)
                        # | 'ReadData transtype tipe' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_Transaction_Type_Time_20210721_192925.txt', skip_header_lines =1)
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-Transaction_Type_Time_202108021.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-Transaction_Type_Time_202108023.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-Transaction_Type_Time_202108022.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype tipe' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/Transaction_Type_Time_20210730_090714.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype tipe' >> beam.io.ReadFromText('gs://stbck_sales_data/3D_data_source_20210501/3d_data_source_Transaction_Type_Time__20210716_155124.txt', skip_header_lines =1)
                        | 'SplitData transtype tipe' >> beam.Map(lambda x: x.split('|'))
                        | 'FormatToDict transtype tipe' >> beam.Map(lambda x: {
                            "date": x[0].strip(),
                            "timetrans": x[1].strip(),
                            "datetime_trans": None,
                            "transaction_number": x[2].strip(),
                            "type_trans": x[3].strip()
                            }) 
                        # | 'DeleteIncompleteData transtype tipe' >> beam.Filter(discard_incomplete_transtypetime)
                        | 'ChangeDataType transtype tipe' >> beam.Map(convert_types_transtypetime)
                        | 'DeleteUnwantedData transtype tipe' >> beam.Map(del_unwanted_cols_transtime)
                        # | 'Write transtype tipe' >> WriteToText('output/transtype_time','.txt')
                        )

# # ======================== STDETAILS FACT : STD >< ST >< BSAP >< DTDO >< MICROS================================================================================================

    transtypemicros_left = 'transtypemicros_data'
    transtypetime_right = 'transtypetime_data'

    transtypemicros_transtypetime_keys = {
        transtypemicros_left: ['transaction_number'],
        transtypetime_right: ['transaction_number']
        }

    transtypemicros_transtypetime_join = {transtypemicros_left: transtypemicros_data, transtypetime_right: transtypetime_data}

    transtypemicros_transtypetime_output = (transtypemicros_transtypetime_join | 'transtype micros transtype time join' >> Join(left_pcol_name=transtypemicros_left, left_pcol=transtypemicros_data,
                                                                                right_pcol_name=transtypetime_right, right_pcol=transtypetime_data,
                                                                                join_type='left', join_keys=transtypemicros_transtypetime_keys)
                                                                                | 'add empty column micros' >> beam.ParDo(ttime_add_emty_data())
    )

    #Distinct
    rm_duplicate_micros = (transtypemicros_transtypetime_output  | 'Convert Dict to String' >> beam.Map(lambda x: str(x))
                                    | 'Remove Duplicate Micros' >> beam.Distinct()
                                    | 'Convert String to Dict' >> beam.Map(lambda x : eval(x))
    )

    # ========================================================================

    #  Dorder >< Dtable
    dorder_left = 'daily_order_data'
    dtable_right = 'daily_table_data'

    join_keys = {
        dorder_left: ['branch_id', 'sales_date', 'cash_register_id', 'order_id'],
        dtable_right: ['branch_id', 'sales_date', 'cash_register_id', 'order_id']
        }

    dorder_dtable_join = {dorder_left: daily_order_data, dtable_right: daily_table_data}
    dorder_dtable_output = (dorder_dtable_join | 'dorder dtable join' >> Join(left_pcol_name=dorder_left, left_pcol=daily_order_data,
                                                               right_pcol_name=dtable_right, right_pcol=daily_table_data,
                                                               join_type='left', join_keys=join_keys)     
                                                | 'Filter Zero price value DOrder DTable' >> beam.ParDo(dodt_exclude_zero_price())                                     
                                                # | 'remove field price' >> beam.Map(dodt_removeprice)
    )

    # ==========================================================================

#     # Branchessap >< ST (UDAH RATA)
    st_left = 'sales_transaction_data'
    bsap_right = 'branchessap_data'

    st_bsap_keys = {
        st_left: ['st_branch_id'],
        bsap_right: ['branch_id']
        }

    st_bsap_join = {st_left: sales_transaction_data, bsap_right: branchessap_data}

    st_bsap_output = (st_bsap_join      | 'st branchessap join' >> Join(left_pcol_name=st_left, left_pcol=sales_transaction_data,
                                                               right_pcol_name=bsap_right, right_pcol=branchessap_data,
                                                               join_type='left', join_keys=st_bsap_keys)
                                        | 'delete columns st branchessap' >> beam.Map(del_unwanted_cols_st_branchessap)
    )

    # # # BRANC DODT
    # st_bsap_left = 'st_bsap_output'
    # dodt_right = 'dorder_dtable_output'

    # st_bsap_dodt_keys = {
    #     st_bsap_left: ['branch_id'],
    #     dodt_right: ['branch_id']
    # }

    # st_bsap_dodt_join = {st_bsap_left: st_bsap_output, dodt_right: dorder_dtable_output}

    # st_bsap_dodt_output = (st_bsap_dodt_join  | 'st bsap dodt join' >> Join(left_pcol_name=st_bsap_left, left_pcol=st_bsap_output,
    #                                                            right_pcol_name=dodt_right, right_pcol=dorder_dtable_output,
    #                                                            join_type='left', join_keys=st_bsap_dodt_keys)                                  
    # )

    # Sales Transaction Detail >< STBSAP (UDAH RATA)
    std_left = 'sales_transaction_details_data'
    st_bsap_right = 'st_bsap_output'

    std_st_bsap_keys = {
        std_left: ['transaction_number'],
        st_bsap_right: ['transaction_number']
    }

    std_st_bsap_join = {std_left: sales_transaction_details_data, st_bsap_right: st_bsap_output}

    std_st_bsap_output = (std_st_bsap_join  | 'std st bsap join' >> Join(left_pcol_name=std_left, left_pcol=sales_transaction_details_data,
                                                               right_pcol_name=st_bsap_right, right_pcol=st_bsap_output,
                                                               join_type='left', join_keys=std_st_bsap_keys)                                  
    )

    # Sales Transaction Detail >< MICROS
    std_st_bsap_left = 'std_st_bsap_output'
    factmicros_right = 'rm_duplicate_micros'

    std_bsap_micros_keys = {
        std_st_bsap_left: ['transaction_number'],
        factmicros_right: ['transaction_number']
        }

    std_bsap_micros_join = {std_st_bsap_left: std_st_bsap_output, factmicros_right: rm_duplicate_micros}

    std_bsap_micros_output = (std_bsap_micros_join | 'fact join 2' >> Join(left_pcol_name=std_st_bsap_left, left_pcol=std_st_bsap_output,
                                                               right_pcol_name=factmicros_right, right_pcol=rm_duplicate_micros,
                                                               join_type='left', join_keys=std_bsap_micros_keys)
                                | 'ADD EMPTY DTYPE TRANS 2' >> beam.ParDo(mt_add_empty_data())
    )


    # # rm_duplicate_dodt = (dorder_dtable_output  | 'Convert DODT Dict to String' >> beam.Map(lambda x: str(x))
    # #                                 | 'Remove Duplicate DODT' >> beam.Distinct()
    # #                                 | 'Convert String to Dict DODT ' >> beam.Map(lambda x : eval(x))
    # # )                                                    

    # STD LEFT >< DODT
    std_bsap_left = 'std_bsap_micros_output'
    dodt_right = 'dorder_dtable_output'

    std_dodt_key = {
        std_bsap_left: ['transaction_number', 'plu'],
        dodt_right: ['transaction_number', 'plu']
        }
        # order_idx stdetail_seq

    fact_join = {std_bsap_left: std_bsap_micros_output, dodt_right: dorder_dtable_output}

    fact_std = (fact_join | 'fact join' >> Join(left_pcol_name=std_bsap_left, left_pcol=std_bsap_micros_output,
                                                               right_pcol_name=dodt_right, right_pcol=dorder_dtable_output,
                                                               join_type='left', join_keys=std_dodt_key)
                                                # | 'Add column dodt' >> 
                                            # | 'Replace price = net price' >> beam.ParDo(replaceprice())
                                
    )

    # # std_bsap_dt_left = 'fact_std'
    # # do_right = 'daily_order_data'
    

    # # std_bsap_dt_left_key = {
    # #     std_bsap_dt_left: ['branch_id', 'sales_date', 'cash_register_id', 'order_id'],
    # #     do_right: ['branch_id', 'sales_date', 'cash_register_id', 'order_id']
    # #     }
    # #     # order_idx stdetail_seq

    # # std_bsap_dt_do_join = {std_bsap_dt_left: fact_std, do_right: daily_order_data}

    # # std_bsap_dt_do = (std_bsap_dt_do_join | 'fact join 3' >> Join(left_pcol_name=std_bsap_dt_left, left_pcol=fact_std,
    # #                                                            right_pcol_name=do_right, right_pcol=daily_order_data,
    # #                                                            join_type='left', join_keys=std_bsap_dt_left_key)
    # #                                             # | 'Add column dodt' >> 
    # #                                         # | 'Replace price = net price' >> beam.ParDo(replaceprice())
                                
    # # )

    # # Distinct
    # # rm_duplicate_dodt = (fact_output  | 'Convert DODT Dict to String' >> beam.Map(lambda x: str(x))
    # #                                 | 'Remove Duplicate DODT' >> beam.Distinct()
    # #                                 | 'Convert String to Dict DODT ' >> beam.Map(lambda x : eval(x))
    # # ) 

    # # replace_price = (fact_output | 'Replace price = net price' >> beam.ParDo(replaceprice()))
    # # delete_column_dodt_fact =  fact_output | 'delete unused column' >> beam.Map(delColDodtFact)

    # CREATE JOIN STDETAIL BSAP DIM DTABLE DORDER MICRO TIME PART DISCOUNT KEY
    stdbaspdtdomt_pd_data = 'fact_std'
    pd_data = 'partdics_data'

    stdbaspdtdomtpd_key = {
        stdbaspdtdomt_pd_data: ['transaction_number','plu', 'branch_id'],
        pd_data: ['transaction_number','plu', 'branch_id']
    }

    join_stdbaspdtdomtpd_dict = {stdbaspdtdomt_pd_data: fact_std, pd_data: partdics_data}

    # JOINED PCOLLECTIONS STDETAIL DTABLE DORDER MICRO TIME PART DISCOUNT
    left_join_stdbaspdtdomtpd = (join_stdbaspdtdomtpd_dict | 'Left Join STD DTDO MICRO TIME PART DISC' >> Join(left_pcol_name=stdbaspdtdomt_pd_data, left_pcol=fact_std,
                                                                    right_pcol_name=pd_data, right_pcol=partdics_data,
                                                                    join_type='left', join_keys=stdbaspdtdomtpd_key)
                                                            | 'ADD EMPTY DATE FOR PART DISCOUNT' >> beam.ParDo(pdisc_add_empty_data())
                                                            # | 'Delete qty' >> beam.Map(del_qty)
    )

    #Distinct
    rm_duplicate_fact = (left_join_stdbaspdtdomtpd  | 'Convert STD FACT Dict to String' >> beam.Map(lambda x: str(x))
                                    | 'Remove Duplicate STD FACT' >> beam.Distinct()
                                    | 'Convert String to Dict STD FACT ' >> beam.Map(lambda x : eval(x))
    ) 
       
# ==================================================================================================

    # # Write to disk
    # dorder_dtable_output | 'write stdetail >< st >< item_master' >> beam.io.WriteToText('data_source_BOH/output/120821-stdleft-dtdo-fact-3kidx-leftjoin-active','.txt')
    rm_duplicate_fact | 'write stdetail >< st >< item_master' >> beam.io.WriteToText('data_source_BOH/output/130821-stbsapdodt5','.txt')

    # Write to BQ STDETAILS FACT
    # fact_output | 'Write to BQ STDETAIL FACT' >> beam.io.WriteToBigQuery(
    #                 table=table_id_dodt,
    #                 dataset=dataset_id,
    #                 project=project_id,
    #                 schema=schema_dodt_fact,
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

    # $env:GOOGLE_APPLICATION_CREDENTIALS="E:/wired-glider-289003-9bd74e62ec18.json"

    # =========================================================================