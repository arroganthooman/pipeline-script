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
            data['transaction_date'] = None
            data['copy_date_time'] = None
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
            "outer": UnnestOuterJoin
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

    #  return list(unique_everseen(data))
    # seen = set()
    # new_l = []
    # for d in data:
    #     t = tuple(sorted(d.items()))
    #     if t not in seen:
    #         seen.add(t)
    #         new_l.append(d)
    # return new_l

    dictstr_set = set()
    for d in data:
        dictstr_set.add(str(d))
    
    dict_list = []
    for d in dictstr_set:
        dict_list.append(eval(d))
    
    return dict_list

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

class tolistmanual(beam.DoFn):
    def process(self, data):
        return [{data}]
    


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
    # copy_date_time = datetime.datetime.strptime(data['copy_date'], date_format3)
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
    # data['micros_transaction_number'] = str(data['micros_transaction_number']) if 'micros_transaction_number' in data else None
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
    data['st_year'] = str(st_transaction_date.year)
    data['st_month'] = str(st_transaction_date.month)
    data['st_day'] = str(st_transaction_date.day)
    data['st_dayname'] = str(st_transaction_date.strftime("%A"))
    data['st_week'] = str(st_transaction_date.strftime("%W"))
    data['st_total_discount'] = float(data['st_total_discount']) if 'st_total_discount' in data else None
    data['st_branch_id'] = str(data['st_branch_id']) if 'st_branch_id' in data else None
    data['st_cash_register_id'] = int(data['st_cash_register_id']) if 'st_cash_register_id' in data else None
    data['st_total_paid'] = float(data['st_total_paid']) if 'st_total_paid' in data else None
    data['st_net_price'] = float(data['st_net_price']) if 'st_net_price' in data else None
    data['st_tax'] = float(data['st_tax']) if 'st_tax' in data else None
    data['st_flag_return'] = int(data['st_flag_return']) if 'st_flag_return' in data else None
    data['st_register_return'] = int(data['st_register_return']) if 'st_register_return' in data else None
    # data['st_trans_date_return'] = str(data['st_trans_date_return']) if 'st_trans_date_return' in data else None
    st_trans_date_return = datetime.datetime.strptime(data['st_trans_date_return'], date_format) 
    data['st_trans_date_return'] = str(st_trans_date_return.date()) 
    data['st_trans_num_return'] = str(data['st_trans_num_return']) if 'st_trans_num_return' in data else None
    data['st_trans_time'] = str(data['st_trans_time']) if 'st_trans_time' in data else None
    data['st_store_type'] = str(data['st_store_type']) if 'st_store_type' in data else None
    # data['st_receiveon'] = str(data['st_receiveon']) if 'st_receiveon' in data else None
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

    data['stdetail_qty'] = int(data['stdetail_qty']) if 'stdetail_qty' in data else None #ada nilai negatif (-1) gabisa di convert ke int
    # data['stdetail_qty'] = float(data['stdetail_qty']) if 'stdetail_qty' in data else None #ada nilai negatif (-1) gabisa di convert ke int

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
    # del data['st_micros_transaction_number']
    return data

def del_unwanted_cols_daily_order(data):
    """Delete the unwanted columns"""
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
    # del data['st_transaction_date']
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

schema_itemmaster_master = (
    'plu:STRING,\
    item_burui:STRING,\
    item_description:STRING,\
    item_article_code:STRING,\
    item_long_description:STRING'
    )

schema_payment_master = (
    'payment_types:INTEGER,\
    paid_payment_description:STRING,\
    paid_types:STRING,\
    card_code:STRING'
    )

schema_branchessap_master = (
    'branch_id:STRING,\
    branch_sap:STRING,\
    branch_name:STRING,\
    branch_profile:INTEGER'
    )

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

# schema_dodt_fact=(
#     'sales_date:STRING,\
#     sales_date_date:DATE,\
#     cash_register_id:INTEGER,\
#     order_id:INTEGER,\
#     plu:STRING,\
#     dorder_ordertxt:STRING,\
#     dorder_upref:STRING,\
#     branch_id:INTEGER,\
#     receiveon:STRING,\
#     receiveon_date:DATE,\
#     transaction_number:STRING,\
#     dtable_receiveOn:STRING,\
#     dtable_receiveOn_date:DATE,\
#     micros_transaction_number:STRING'
# )

# schema_micros_fact=(
#     'transaction_number:STRING,\
#     dtype:STRING,\
#     sales_date:DATE,\
#     timetrans:STRING,\
#     type_trans:STRING,\
#     branch_id:INTEGER'
# )

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
schema_stdetails_fact = (
    'transaction_number:STRING,\
    stdetail_seq:INTEGER,\
    plu:STRING,\
    stdetail_item_description:STRING,\
    stdetail_price:FLOAT,\
    stdetail_qty:INTEGER,\
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
    copy_date:DATETIME,\
    copy_date_time:TIME,\
    transaction_date:DATE'
    )

    

# tb_spec='wired-glider-289003:lake.sb-test-paid'
project_id = 'wired-glider-289003'  # replace with your project ID
dataset_id = 'starbuck_data_samples'  # replace with your dataset ID

# project_id = 'ps-id-starbucks-da-05052021'  # replace with your project ID
# dataset_id = 'stbck_sales'  # replace with your dataset ID

table_id_itemmaster = 'SB_ITEMMASTER_MASTER_9D'  # replace with your table ID
table_id_payment = 'SB_PAYMENT_MASTER_9D'  # replace with your table ID
table_id_branchessap = 'SB_BRANCHESSAP_MASTER_9D'  # replace with your table ID
table_id_st = 'SB_ST_FACT_1M'  # replace with your table ID
# table_id_dodt = 'SB_DODT_FACT'  # replace with your table ID
# table_id_micros = 'SB_MICROS_FACT'  # replace with your table ID
table_id_paid = 'SB_PAID_FACT_3D'  # replace with your table ID
table_id_paid_item = 'SB_PAID_ITEM_9D'
table_id_stdetails = 'SB_STDETAIL_FACT_ALL_1M'  # replace with your table ID
	
    # project_id = "wired-glider-289003"  # replace with your project ID
    # dataset_id = 'lake'  # replace with your dataset ID
    # table_id = 'sb-test'  # replace with your table ID

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
    # gs://sb-data-source
# with beam.Pipeline(options=PipelineOptions) as p:
    branchessap_data = (p 
                        # | 'ReadData branchessap type' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_BranchesSap_20210607_093034.txt', skip_header_lines =1)
                        # | 'ReadData branchessap type' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_BranchesSap_20210710_065124.txt', skip_header_lines =1)
                        # | 'ReadData branchessap type' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_BranchesSap_20210721_190925.txt', skip_header_lines =1)
                        | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/BranchesSap_20210607_093034.txt', skip_header_lines =1)
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_BranchesSap_20210710_065124.txt', skip_header_lines =1)
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_BranchesSap_20210721_190925.txt', skip_header_lines =1)
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/BranchesSap_20210728_140524.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-BranchesSap_20210801_211616.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-BranchesSap_20210801_213849.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData Branches_sap' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-BranchesSap_20210801_220059.txt', skip_header_lines =1, coder=CustomCoder())
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

    # payment_type_data = (p
    #                     # | 'ReadData payment type' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_Payment_Types_20210607_093448.txt', skip_header_lines =1) 
    #                     # | 'ReadData payment type' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_Payment_Types_20210710_065029.txt', skip_header_lines =1)
    #                     # | 'ReadData payment type' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_Payment_Types_20210721_190833.txt', skip_header_lines =1)
                        # | 'ReadData payment type' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/Payment_Types_20210607_093448.txt', skip_header_lines =1)
    #                     # | 'ReadData payment type' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_Payment_Types_20210710_065029.txt', skip_header_lines =1)
    #                     # | 'ReadData payment type' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_Payment_Types_20210721_190833.txt', skip_header_lines =1)
    #                     # | 'ReadData payment type' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/Payment_Types_20210728_140501.txt', skip_header_lines =1)
    #                     | 'ReadData payment type' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-Payment_Types_20210801_211616.txt', skip_header_lines =1)
    #                     # | 'ReadData payment type' >> beam.io.ReadFromText('gs://stbck_sales_data/3D_data_source_20210501/3d_data_source_Payment_Types_20210710_065029.txt', skip_header_lines =1)
    #                     | 'SplitData payment type' >> beam.Map(lambda x: x.split('|'))
    #                     | 'FormatToDict payment type' >> beam.Map(lambda x: {
    #                         "payment_types": x[0],
    #                         "paid_payment_description": x[1].strip(),
    #                         "paid_types": x[2].strip(),
    #                         "Seq": x[3],
    #                         "card_code": x[4].strip()
    #                         }) 
    #                     # | 'DeleteIncompleteData payment type' >> beam.Filter(discard_incomplete_payment_types)
    #                     | 'ChangeDataType payment type' >> beam.Map(convert_types_payment_types)
    #                     | 'DeleteUnwantedData payment type' >> beam.Map(del_unwanted_cols_payment_types)
    #                     # | 'Write payment type' >> WriteToText('data_source_BOH/output/data-payment_types','.txt')
    #                     )

    transtypemicros_data = (p 
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_Transaction_Type_Micros_20210611_124914.txt', skip_header_lines =1)
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_Transaction_Type_Micros_20210716_134914.txt', skip_header_lines =1)
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_Transaction_Type_Micros_20210721_193526.txt', skip_header_lines =1)
                        | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/data_source_20210501_Transaction_Type_Micros_20210611_124914.txt', skip_header_lines =1)
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_Transaction_Type_Micros_20210716_134914.txt', skip_header_lines =1)
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_Transaction_Type_Micros_20210721_193526.txt', skip_header_lines =1)
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/Transaction_Type_Micros_20210730_090844.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-Transaction_Type_Micros_202108021.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-Transaction_Type_Micros_202108023.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype micros' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-Transaction_Type_Micros_202108022.txt', skip_header_lines =1, coder=CustomCoder())
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
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_Transaction_Type_Time_20210607_102017.txt', skip_header_lines =1)
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_Transaction_Type_Time__20210716_155124.txt', skip_header_lines =1)
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_Transaction_Type_Time_20210721_192925.txt', skip_header_lines =1)
                        | 'ReadData transtype time' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/Transaction_Type_Time_20210607_102017.txt', skip_header_lines =1)
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_Transaction_Type_Time__20210716_155124.txt', skip_header_lines =1)
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_Transaction_Type_Time_20210721_192925.txt', skip_header_lines =1)
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/Transaction_Type_Time_20210730_090714.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-Transaction_Type_Time_202108021.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-Transaction_Type_Time_202108023.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-Transaction_Type_Time_202108022.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transtype time' >> beam.io.ReadFromText('gs://stbck_sales_data/3D_data_source_20210501/3d_data_source_Transaction_Type_Time__20210716_155124.txt', skip_header_lines =1)
                        | 'SplitData transtype time' >> beam.Map(lambda x: x.split('|'))
                        | 'FormatToDict transtype time' >> beam.Map(lambda x: {
                            "date": x[0].strip(),
                            "timetrans": x[1].strip(),
                            "datetime_trans": None,
                            "transaction_number": x[2].strip(),
                            "type_trans": x[3].strip()
                            }) 
                        # | 'DeleteIncompleteData transtype time' >> beam.Filter(discard_incomplete_transtypetime)
                        | 'ChangeDataType transtype time' >> beam.Map(convert_types_transtypetime)
                        | 'DeleteUnwantedData transtype time' >> beam.Map(del_unwanted_cols_transtime)
                        # | 'Write transtype time' >> WriteToText('output/transtype_time','.txt')
                        )

    partdics_data = (p 
                        # | 'ReadData partdics ' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_part_disc_20210607_095311.txt', skip_header_lines =1)
                        # | 'ReadData partdics' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_part_disc_20210710_064602.txt', skip_header_lines =1)
                        # | 'ReadData partdics' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_part_disc_20210721_185935.txt', skip_header_lines =1)
                        | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/part_disc_20210607_095311.txt', skip_header_lines =1)
                        # | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_part_disc_20210710_064602.txt', skip_header_lines =1)
                        # | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_part_disc_20210721_185935.txt', skip_header_lines =1)
                        # | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/part_disc_20210728_140151.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-part_disc_20210801_210649.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-part_disc_20210801_212643.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData partdisc' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-part_disc_20210801_214935.txt', skip_header_lines =1, coder=CustomCoder())
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

    # paid_data = (p 
    #                     # | 'ReadData paid' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_paid_20210607_095120.txt', skip_header_lines =1)
    #                     # | 'ReadData paid' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_paid_20210710_064714.txt', skip_header_lines =1)
    #                     # | 'ReadData paid' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_paid_20210721_190012.txt', skip_header_lines =1)
    #                     # | 'ReadData paid' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/paid_20210607_095120.txt', skip_header_lines =1)
    #                     # | 'ReadData paid' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_paid_20210710_064714.txt', skip_header_lines =1)
    #                     # | 'ReadData paid' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_paid_20210721_190012.txt', skip_header_lines =1)
    #                     # | 'ReadData paid' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/paid_20210728_140218.txt', skip_header_lines =1)
    #                     | 'ReadData paid' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-paid_20210801_210652.txt', skip_header_lines =1)
    #                     # | 'ReadData paid' >> beam.io.ReadFromText('gs://stbck_sales_data/3D_data_source_20210501/3d_data_source_paid_20210710_064714.txt', skip_header_lines =1)
    #                     | 'SplitData paid' >> beam.Map(lambda x: x.split('|'))
    #                     | 'FormatToDict paid' >> beam.Map(lambda x: {
    #                         "transaction_number": x[0],
    #                         "payment_types": x[1],
    #                         "seq": x[2],
    #                         "Currency_ID": x[3],
    #                         "Currency_Rate": x[4],
    #                         "Credit_Card_No": x[5],
    #                         "Credit_Card_Name": x[6],
    #                         "paid_paid_amount": x[7],
    #                         "Shift": x[8],
    #                         "micros_transaction_number": x[9]
    #                         }) 
    #                     # | 'DeleteIncompleteData paid' >> beam.Filter(discard_incomplete_paid)
    #                     | 'ChangeDataType paid' >> beam.Map(convert_types_paid)
    #                     | 'DeleteUnwantedData paid' >> beam.Map(del_unwanted_cols_paid)
    #                     # | 'Write paid' >> WriteToText('data_source_BOH/output/paid','.txt')
    #                     )

    sales_transaction_data = (p 
                        # | 'ReadData transaction' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_sales_transactions_20210607_094416.txt', skip_header_lines =1)
                        # | 'ReadData transaction' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_sales_transactions_20210709_102056.txt', skip_header_lines =1)
                        # | 'ReadData transaction' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_sales_transactions_20210721_184841.txt', skip_header_lines =1)
                        | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/sales_transactions_20210607_094416.txt', skip_header_lines =1)
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_sales_transactions_20210709_102056.txt', skip_header_lines =1)
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_sales_transactions_20210721_184841.txt', skip_header_lines =1)
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/sales_transactions_20210728_132936.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-sales_transactions_20210801_210110.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-sales_transactions_20210801_212107.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-sales_transactions_20210801_214416.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction' >> beam.io.ReadFromText('gs://stbck_sales_data/3D_data_source_20210501/3d_data_source_sales_transactions_20210709_102056.txt', skip_header_lines =1)
                        | 'SplitData transaction' >> beam.Map(lambda x: x.split('|'))
                        | 'FormatToDict transaction' >> beam.Map(lambda x: {
                            "transaction_number": x[0],
                            "st_cashier_id": x[1],
                            "st_customer_id": x[2].strip(),
                            "Card_Number": x[3], #delete
                            "st_spending_program_id": x[4].strip(),
                            "st_transaction_date": x[5],
                            "st_year": None,
                            "st_month": None,
                            "st_day": None,
                            "st_dayname": None,
                            "st_week": None,
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
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_sales_transaction_details_20210607_094608.txt', skip_header_lines =1)
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_sales_transaction_details_20210709_135236.txt', skip_header_lines =1)
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_sales_transaction_details_20210721_185124.txt', skip_header_lines =1)
                        | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/sales_transaction_details_20210607_094608.txt', skip_header_lines =1)
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_sales_transaction_details_20210709_135236.txt', skip_header_lines =1)
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_sales_transaction_details_20210721_185124.txt', skip_header_lines =1)
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/sales_transaction_details_20210728_133857.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-sales_transaction_details_20210801_210422.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-sales_transaction_details_20210801_212400.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData transaction details' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-sales_transaction_details_20210801_214652.txt', skip_header_lines =1, coder=CustomCoder())
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
                            "part_disc": None
                            }) 
                        # | 'DeleteIncompleteData transaction' >> beam.Filter(discard_incomplete_sales_transaction)
                        | 'ChangeDataType transaction details' >> beam.Map(convert_types_sales_transaction_details)
                        # | 'DeleteUnwantedData transaction details' >> beam.Map(del_unwanted_cols_sales_transaction_details)
                        # | 'format data transaction' >> beam.Map(changenulltonodata)
                        # | 'Write sales transaction details' >> WriteToText('data_source_BOH/output/data-sales_transaction_details','.txt')
                        )

    daily_table_data = (p 
                        # | 'ReadData daily table' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_daily_table_20210607_094807.txt', skip_header_lines =1)
                        # | 'ReadData daily table' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_daily_table_20210710_065220.txt', skip_header_lines =1)
                        # | 'ReadData daily table' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_daily_table_20210721_190936.txt', skip_header_lines =1)
                        | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/daily_table_20210607_094807.txt', skip_header_lines =1)
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_daily_table_20210710_065220.txt', skip_header_lines =1)
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_daily_table_20210721_190936.txt', skip_header_lines =1)
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/daily_table_20210728_140540.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-daily_table_20210801_211053.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-daily_table_20210801_212940.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily table' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-daily_table_20210801_215201.txt', skip_header_lines =1, coder=CustomCoder())
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
                        # | 'Write daily table' >> WriteToText('data_source_BOH/output/daily_table','.txt')
                        )

    daily_order_data = (p 
                        # | 'ReadData daily order' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_daily_order_20210607_094920.txt', skip_header_lines =1)
                        # | 'ReadData daily order' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_daily_order_20210710_065323.txt', skip_header_lines =1)
                        # | 'ReadData daily order' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_daily_order_20210721_191008.txt', skip_header_lines =1)
                        | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/daily_order_20210607_094920.txt', skip_header_lines =1)
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_daily_order_20210710_065323.txt', skip_header_lines =1)
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_daily_order_20210721_191008.txt', skip_header_lines =1)
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/daily_order_20210728_140624.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-daily_order_20210801_211102.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-11-20days/11-20days-daily_order_20210801_212949.txt', skip_header_lines =1, coder=CustomCoder())
                        # | 'ReadData daily order' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-21-31days/21-31days-daily_order_20210801_215211.txt', skip_header_lines =1, coder=CustomCoder())
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

    # item_master_data = (p 
    #                     # | 'ReadData item master' >> beam.io.ReadFromText('data_source_BOH/data_source_20210501_item_master_20210607_093216.txt', skip_header_lines =1, coder=CustomCoder())
    #                     # | 'ReadData item master' >> beam.io.ReadFromText('BOH_Data_20210501_03/3d_data_source_item_master_20210710_065110.txt', skip_header_lines =1, coder=CustomCoder())
    #                     # | 'ReadData item master' >> beam.io.ReadFromText('BOH_Data_20210501_09/9d_data_source_item_master_20210721_190846.txt', skip_header_lines =1, coder=CustomCoder())
    #                     # | 'ReadData item master' >> beam.io.ReadFromText('gs://sb-data-source/data_source_20210501/item_master_20210607_093216.txt', skip_header_lines =1, coder=CustomCoder())
    #                     # | 'ReadData item master' >> beam.io.ReadFromText('gs://sb-data-source/3d_data_source_20210501/3d_data_source_item_master_20210710_065110.txt', skip_header_lines =1, coder=CustomCoder())
    #                     # | 'ReadData item master' >> beam.io.ReadFromText('gs://sb-data-source/9d_data_source_20210501/9d_data_source_item_master_20210721_190846.txt', skip_header_lines =1, coder=CustomCoder())
    #                     # | 'ReadData item master' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource_20210501/item_master_20210728_140512.txt', skip_header_lines =1, coder=CustomCoder())
    #                     | 'ReadData item master' >> beam.io.ReadFromText('gs://sb-data-source/1m_datasource-20210501-01-10days/01-10days-item_master_20210801_211616.txt', skip_header_lines =1, coder=CustomCoder())
    #                     # | 'ReadData item master' >> beam.io.ReadFromText('gs://stbck_sales_data/3D_data_source_20210501/3d_data_source_item_master_20210710_065110.txt', skip_header_lines =1, coder=CustomCoder())
    #                     | 'SplitData item master' >> beam.Map(lambda x: x.split('|'))
    #                     | 'FormatToDict item master' >> beam.Map(lambda x: {
    #                         "Branch_ID": x[0],
    #                         "plu": x[1].strip(),
    #                         "Class": x[2],
    #                         "item_burui": x[3],
    #                         "DP2": x[4],
    #                         "Supplier_Code": x[5],
    #                         "SPLU": x[6],
    #                         "item_description": x[7].strip(),
    #                         "Normal_Price": x[8],
    #                         "Current_Price": x[9],
    #                         "Disc_Percent": x[10],
    #                         "MinQty4Disc": x[11],
    #                         "Point": x[12],
    #                         "Constraint_Qty": x[13],
    #                         "Constraint_Amt": x[14],
    #                         "Constraint_Point": x[15],
    #                         "Constraint_Flag": x[16],
    #                         "Point_Item_Flag": x[17],
    #                         "Point_Spending_Flag": x[18],
    #                         "Get_Class_Flag": x[19],
    #                         "Flag": x[20],
    #                         "Margin": x[21],
    #                         "Last_Update": x[22], # ini ada date nya
    #                         "Unit_of_Measure": x[23],
    #                         "item_article_code": x[24].strip(),
    #                         "item_long_description": x[25],
    #                         "Brand": x[26],
    #                         "Package": x[27],
    #                         "Perishable": x[28],
    #                         "Store_Type": x[29]
    #                         }) 
    #                     # | 'DeleteIncompleteData transaction' >> beam.Filter(discard_incomplete_sales_transaction)
    #                     | 'ChangeDataType item master' >> beam.Map(convert_types_item_master)
    #                     | 'DeleteUnwantedData item master' >> beam.Map(del_unwanted_cols_item_master)
    #                     # | 'format data transaction' >> beam.Map(changenulltonodata)
    #                     # | 'Write item master' >> WriteToText('gs://sb-data-source/output/item_master_encoding_runfromlokal140621','.txt')
    #                     # | 'Write item master' >> WriteToText('data_source_BOH/output/item_master_encoding','.txt')
    #                     )


  # ================= TRANSTYPE MICROS >< TRANSTYPE TIME==============================================================================

    # Transtype micros >< Transtype Time
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

    rmdup_micros = (transtypemicros_transtypetime_output  | 'tolist micros' >> beam.combiners.ToList()
                                    | 'remove duplicate micros' >> beam.Map(rmduplicate)
                                    | 'todict micros' >> beam.ParDo(BreakList())
                                    # | 'Delete unwanted column for Table Fact' >> beam.Map(del_unwanted_cols_fact)  
    )



# ======================== STDETAILS FACT : STD >< ST >< BSAP >< DTDO >< MICROS================================================================================================

    # Branchessap >< ST (UDAH RATA)
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

#     # Dorder >< Dtable (UDAH RATA), function ok but upref (lt.wc, SL) blm keambil

    dtable_left = 'daily_table_data'
    dorder_right = 'daily_order_data'

    join_keys = {
        dtable_left: ['branch_id', 'sales_date', 'cash_register_id', 'order_id'],
        dorder_right: ['branch_id', 'sales_date', 'cash_register_id', 'order_id']
        }

    dorder_dtable_join = {dtable_left: daily_table_data, dorder_right: daily_order_data}
    dorder_dtable_output = (dorder_dtable_join | 'dorder dtable join' >> Join(left_pcol_name=dtable_left, left_pcol=daily_table_data,
                                                               right_pcol_name=dorder_right, right_pcol=daily_order_data,
                                                               join_type='left', join_keys=join_keys)     
                                                | 'Filter Zero price value DOrder DTable' >> beam.ParDo(dodt_exclude_zero_price())                                     
    )

    rmdup_dtdo = (dorder_dtable_output  | 'tolist dtdo' >> beam.combiners.ToList()
                                    | 'remove dtdo' >> beam.Map(rmduplicate)
                                    | 'todict dtdo' >> beam.ParDo(BreakList())
                                    # | 'Delete unwanted column for Table Fact' >> beam.Map(del_unwanted_cols_fact)  
    )

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

    # ST Details >< DODT
    st_stdetails_left = 'std_st_bsap_output'
    dodt_right = 'rmdup_dtdo'

    stdetails_dodt_keys = {
        st_stdetails_left: ['transaction_number','plu'],
        dodt_right: ['transaction_number','plu']
        }

    stdetails_dodt_join = {st_stdetails_left: std_st_bsap_output, dodt_right: rmdup_dtdo}
    
    stdetails_dodt_output = (stdetails_dodt_join | 'stdetails st item_master joinnnn' >> Join(left_pcol_name=st_stdetails_left, left_pcol=std_st_bsap_output,
                                                                    right_pcol_name=dodt_right, right_pcol=rmdup_dtdo,
                                                                    join_type='left', join_keys=stdetails_dodt_keys)
    )

#     rmdup_std_dodt = (stdetails_dodt_output  | 'tolist dodt' >> beam.combiners.ToList()
#                                     | 'remove dodt' >> beam.Map(rmduplicate)
#                                     | 'todict dodt' >> beam.ParDo(BreakList())
#                                     # | 'Delete unwanted column for Table Fact' >> beam.Map(del_unwanted_cols_fact)  
#     )

# #     # all_fact_column = rmdup_std_dodt | 'Delete unwanted column for Table Fact' >> beam.Map(del_unwanted_cols_fact)                                                                               

#     # test
    factdtdo_left = 'stdetails_dodt_output'
    factmicros_right = 'rmdup_micros'

    fact_keys = {
        factdtdo_left: ['transaction_number'],
        factmicros_right: ['transaction_number']
        }

    fact_join = {factdtdo_left: stdetails_dodt_output, factmicros_right: rmdup_micros}

    fact_output = (fact_join | 'fact join' >> Join(left_pcol_name=factdtdo_left, left_pcol=stdetails_dodt_output,
                                                               right_pcol_name=factmicros_right, right_pcol=rmdup_micros,
                                                               join_type='left', join_keys=fact_keys)
                                | 'ADD EMPTY DTYPE TRANS' >> beam.ParDo(mt_add_empty_data())
    )

    # rmdup_fact = (fact_output  | 'tolist fact' >> beam.combiners.ToList()
    #                                 | 'fact dup' >> beam.Map(rmduplicate)
    #                                 | 'todict fact' >> beam.ParDo(BreakList())
    # )                                      

    # CREATE JOIN STDETAIL BSAP DIM DTABLE DORDER MICRO TIME PART DISCOUNT KEY
    stdbaspdtdomt_pd_data = 'fact_output'
    pd_data = 'partdics_data'
    stdbaspdtdomtpd_key = {
        stdbaspdtdomt_pd_data: ['transaction_number','plu', 'branch_id'],
        pd_data: ['transaction_number','plu', 'branch_id']
    }
    join_stdbaspdtdomtpd_dict = {stdbaspdtdomt_pd_data: fact_output, pd_data: partdics_data}

    # JOINED PCOLLECTIONS STDETAIL DTABLE DORDER MICRO TIME PART DISCOUNT
    left_join_stdbaspdtdomtpd = (join_stdbaspdtdomtpd_dict | 'Left Join STD DTDO MICRO TIME PART DISC' >> Join(left_pcol_name=stdbaspdtdomt_pd_data, left_pcol=fact_output,
                                                                    right_pcol_name=pd_data, right_pcol=partdics_data,
                                                                    join_type='left', join_keys=stdbaspdtdomtpd_key)
                                                            | 'ADD EMPTY DATE FOR PART DISCOUNT' >> beam.ParDo(pdisc_add_empty_data())
                                                            # | 'Delete unwanted column for Table Fact' >> beam.Map(del_unwanted_cols_fact)
    )

    # Run Local first
    fact_fact = (left_join_stdbaspdtdomtpd  | 'tolist fact last' >> beam.combiners.ToList()
                                    | 'fact dup last' >> beam.Map(rmduplicate)
                                    | 'todict fact last' >> beam.ParDo(BreakList())
                                    | 'Delete unwanted column for Table Fact' >> beam.Map(del_unwanted_cols_fact)
                                    # | 'WriteToList to text' >> WriteToText('hasil-fact-last', '.txt')
    )

    # Run use DataFlow second
    # processed_list = process_file() # -> pake method untuk open file hasil ToList() dan diubah ke list of dict & remove duplicate
    # fact_fact = (left_join_stdbaspdtdomtpd  | 'Create from ToList() fact last' >> beam.Create(processed_list)
    #                                 # | 'fact dup last' >> beam.Map(rmduplicate)
    #                                 # | 'todict fact last' >> beam.ParDo(BreakList())
    #                                 | 'Delete unwanted column for Table Fact' >> beam.Map(del_unwanted_cols_fact)
    # )
    
#    # ================PAID >< PAYMENT TYPES=======================================================================

    # Paid >< Payment 
    # paid_left = 'paid_data'
    # payment_type_right = 'payment_type_data'

    # paid_payment_keys = {
    #     paid_left: ['payment_types'],
    #     payment_type_right: ['payment_types']
    #     }

    # paid_payment_join = {paid_left: paid_data, payment_type_right: payment_type_data}

    # paid_payment_output = paid_payment_join | 'paid payment join' >> Join(left_pcol_name=paid_left, left_pcol=paid_data,
    #                                                            right_pcol_name=payment_type_right, right_pcol=payment_type_data,
    #                                                            join_type='left', join_keys=paid_payment_keys)

    # clean_pay_payttpe = (paid_payment_output | 'CLEAN UNRECOGNISE PAYMENT TYPE' >> beam.ParDo(clean_pay_paymenttype()))
    
    # # Paid >< Payment >< DTDO 
    # paid_payment_left = 'clean_pay_payttpe'
    # dtdo_right = 'dorder_dtable_output'

    # paid_payment_dtdo_keys = {
    #     paid_payment_left: ['transaction_number'],
    #     dtdo_right: ['transaction_number']
    #     }

    # paid_payment_dtdo_join = {paid_payment_left: clean_pay_payttpe, dtdo_right: dorder_dtable_output}

    # paid_payment_dtdo_output = (paid_payment_dtdo_join    | 'paid payment st bsap join' >> Join(left_pcol_name=paid_payment_left, left_pcol=clean_pay_payttpe,
    #                                                            right_pcol_name=dtdo_right, right_pcol=dorder_dtable_output,
    #                                                            join_type='left', join_keys=paid_payment_dtdo_keys)
    # )

    # # PAID FACT >< MICROS
    # paid_fact_left = 'paid_payment_dtdo_output'
    # micros_right = 'transtypemicros_transtypetime_output'

    # paid_micros_keys = {
    #     paid_fact_left: ['transaction_number'],
    #     micros_right: ['transaction_number']
    #     }

    # paid_micros_join = {paid_fact_left: paid_payment_dtdo_output, micros_right: transtypemicros_transtypetime_output}

    # paid_micros_output = (paid_micros_join    | 'paid micros join' >> Join(left_pcol_name=paid_fact_left, left_pcol=paid_payment_dtdo_output,
    #                                                            right_pcol_name=micros_right, right_pcol=transtypemicros_transtypetime_output,
    #                                                            join_type='left', join_keys=paid_micros_keys)
    #                                             | 'ADD EMPTY column paid' >> beam.ParDo(mt_add_empty_data())
    # )

    # # ================================================
    
    # # del_paid_amt = clean_pay_payttpe | 'Del paid_paid_amount' >> beam.Map(del_paid_amount)

    # # Paid >< STD (BELANG)
    # paid_payment_left = 'clean_pay_payttpe'
    # std_right = 'sales_transaction_details_data'

    # paid_payment_std_keys = {
    #     paid_payment_left: ['transaction_number'],
    #     std_right: ['transaction_number']
    #     }

    # paid_payment_std_join = {paid_payment_left: clean_pay_payttpe, std_right: sales_transaction_details_data}

    # paid_payment_std_output = (paid_payment_std_join    | 'paid payment std join' >> Join(left_pcol_name=paid_payment_left, left_pcol=clean_pay_payttpe,
    #                                                            right_pcol_name=std_right, right_pcol=sales_transaction_details_data,
    #                                                            join_type='left', join_keys=paid_payment_std_keys)
    #                                                     # | 'ADD EMPTY column paid std' >> beam.ParDo(paid_std_add_data())
    # )

    # #  PAID PAYMENT STD >< DTDO
    # paid_payment_std_left = 'paid_payment_std_output'
    # # dtdo_right = 'dorder_dtable_output'

    # paid_payment_std_dtdo_keys = {
    #     paid_payment_std_left: ['transaction_number'],
    #     dtdo_right: ['transaction_number']
    #     }

    # paid_payment_std_dtdo_join = {paid_payment_std_left: paid_payment_std_output, dtdo_right: dorder_dtable_output}

    # paid_payment_std_dtdo_output = (paid_payment_std_dtdo_join      | 'paid payment std dodt join' >> Join(left_pcol_name=paid_payment_std_left, left_pcol=paid_payment_std_output,
    #                                                            right_pcol_name=dtdo_right, right_pcol=dorder_dtable_output,
    #                                                            join_type='left', join_keys=paid_payment_std_dtdo_keys)
                                                                     
                                                               
    # )

    # # PAID PAYMENT STD DTDO >< MICROS
    # paid_fact_left = 'paid_payment_std_dtdo_output'
    # # micros_right = 'transtypemicros_transtypetime_output'

    # paid_std_dtdo_micros_keys = {
    #     paid_fact_left: ['transaction_number'],
    #     micros_right: ['transaction_number']
    #     }

    # paid_std_dtdo_micros_join = {paid_fact_left: paid_payment_std_dtdo_output, micros_right: transtypemicros_transtypetime_output}

    # paid_std_dtdo_micros_output = (paid_std_dtdo_micros_join    | 'paid std dtdo micros join' >> Join(left_pcol_name=paid_fact_left, left_pcol=paid_payment_std_dtdo_output,
    #                                                            right_pcol_name=micros_right, right_pcol=transtypemicros_transtypetime_output,
    #                                                            join_type='left', join_keys=paid_std_dtdo_micros_keys)
    # )
       
# ==================================================================================================

    # # Write to disk
    # rmdup_micros | 'write stdetail >< st >< item_masterrr' >> beam.io.WriteToText('data_source_BOH/output/300721-micros_output2','.txt')
    rmdup_dtdo | 'dtdo' >> beam.io.WriteToText('data_source_BOH/output/300721-dtdo_output2','.txt')
    
    # fact_fact | 'write stdetail >< st >< item_master' >> beam.io.WriteToText('data_source_BOH/output/310721-std-fact1d','.txt')
    # partdics_data | 'write stdetail >< st >< item_master' >> beam.io.WriteToText('data_source_BOH/output/290721-data-partdisc','.txt')
    # transtypemicros_transtypetime_output | 'paid fact >< item_master' >> beam.io.WriteToText('C:/Users/Lingga/Documents/output9d/290721-std-fact1d','.txt')
    

    # Write to BQ Item Master
    # item_master_data | 'Write to BQ Item Master' >> beam.io.WriteToBigQuery(
    #                 table=table_id_itemmaster,
    #                 dataset=dataset_id,
    #                 project=project_id,
    #                 schema=schema_itemmaster_master,
    #                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #                 # batch_size=int(100)
    #                 )

    # Write to BQ Payment Types
    # payment_type_data | 'Write to BQ Payment Types' >> beam.io.WriteToBigQuery(
    #                 table=table_id_payment,
    #                 dataset=dataset_id,
    #                 project=project_id,
    #                 schema=schema_payment_master,
    #                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #                 # batch_size=int(100)
    #                 )

    # # Write to BQ Branchessap
    # branchessap_data | 'Write to BQ Branchessap' >> beam.io.WriteToBigQuery(
    #                 table=table_id_branchessap,
    #                 dataset=dataset_id,
    #                 project=project_id,
    #                 schema=schema_branchessap_master,
    #                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #                 # batch_size=int(100)
    #                 )

    # # Write to BQ ST FACT
    # sales_transaction_data | 'Write to BQ ST FACT' >> beam.io.WriteToBigQuery(
    #                 table=table_id_st,
    #                 dataset=dataset_id,
    #                 project=project_id,
    #                 schema=schema_st_fact,
    #                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #                 # batch_size=int(100)
    #                 )

    # # # # Write to BQ DODT FACT
    # # # dorder_dtable_output | 'Write to BQ DODT FACT' >> beam.io.WriteToBigQuery(
    # # #                 table=table_id_dodt,
    # # #                 dataset=dataset_id,
    # # #                 project=project_id,
    # # #                 schema=schema_dodt_fact,
    # # #                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    # # #                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    # # #                 # batch_size=int(100)
    # # #                 )

    # # # # Write to BQ MICROS FACT
    # # # micros_st_bsap_output | 'Write to BQ MICROS FACT' >> beam.io.WriteToBigQuery(
    # # #                 table=table_id_micros,
    # # #                 dataset=dataset_id,
    # # #                 project=project_id,
    # # #                 schema=schema_micros_fact,
    # # #                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    # # #                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    # # #                 # batch_size=int(100)
    # # #                 )

    # Write to BQ PAID FACT
    # paid_micros_output | 'Write to BQ PAID FACT' >> beam.io.WriteToBigQuery(
    #                 table=table_id_paid,
    #                 dataset=dataset_id,
    #                 project=project_id,
    #                 schema=schema_paid_fact,
    #                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #                 # batch_size=int(100)
    #                 )

    # # Write to BQ PAID ITEM
    # paid_all_item_output | 'Write to BQ PAID ITEM' >> beam.io.WriteToBigQuery(
    #                 table=table_id_paid_item,
    #                 dataset=dataset_id,
    #                 project=project_id,
    #                 schema=schema_paid_item,
    #                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #                 # batch_size=int(100)
    #                 )

    # Write to BQ STDETAILS FACT
    # fact_fact | 'Write to BQ STDETAIL FACT' >> beam.io.WriteToBigQuery(
    #                 table=table_id_stdetails,
    #                 dataset=dataset_id,
    #                 project=project_id,
    #                 schema=schema_stdetails_fact,
    #                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
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

    # --autoscaling_algorithm THROUGHPUT_BASED

    # python py-df-sbst01.py --project=wired-glider-289003 --key=E:/wired-glider-289003-9bd74e62ec18.json --staging_location gs://wired-glider-289003/starbucks-BOH/staging --temp_location gs://wired-glider-289003/starbucks-BOH/temp --region=asia-southeast1 --runner=DataflowRunner --save_main_session --job_name lingga-test-sb-boh-strans-branchessap --disk_size_gb=250 --num_workers=2 --max_num_workers=10 --machine_type=n2-standard-4
    # $env:GOOGLE_APPLICATION_CREDENTIALS="E:/wired-glider-289003-9bd74e62ec18.json"

    # =========================================================================
    #  3 DAYS  Data

    # $env:GOOGLE_APPLICATION_CREDENTIALS="E:/Starbucks/py-df-sbst/ps-id-starbucks-da-05052021-6ca2a1e19a46.json"
    # python py-df-sbst01.py --project=ps-id-starbucks-da-05052021 --key=E:/Starbucks/py-df-sbst/ps-id-starbucks-da-05052021-6ca2a1e19a46.json --staging_location gs://ps-id-starbucks-da-05052021/starbucks-BOH/staging --temp_location gs://ps-id-starbucks-da-05052021/starbucks-BOH/temp --region=asia-southeast2 --runner=DataflowRunner --save_main_session --job_name sb-3d

    # python py-df-sbst01.py --project=ps-id-starbucks-da-05052021 --key=E:/Starbucks/py-df-sbst/ps-id-starbucks-da-05052021-6ca2a1e19a46.json --staging_location gs://ps-id-starbucks-da-05052021/starbucks-BOH/staging --temp_location gs://ps-id-starbucks-da-05052021/starbucks-BOH/temp --region=asia-southeast2 --runner=DataflowRunner --save_main_session --job_name sb-3d --disk_size_gb=150 --max_num_workers=10 --machine_type=n2-standard-4