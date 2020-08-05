import numpy as np
import pandas as pd
from math import ceil
import psycopg2
from time import time
import psycopg2.extras
from copy import copy

sqluser = 'postgres'
dbname = 'test'
schema_name = 'mimiciii'
password = 'postgres'
db_schema = 'set search_path to ' + schema_name + ';'
con = psycopg2.connect(dbname=dbname, user=sqluser, password=password, host='/var/run/postgresql/')
cursor = con.cursor()

to_use_ce = pd.read_csv('ce_use.csv')
all_subs_lst = pd.read_sql('select subject_id from patients;', con)['subject_id'].tolist()

def any_in_postgres(lst):
    if type(lst) in (np.array, np.ndarray, pd.Series):
        lst = lst.tolist()
    elif type(lst) is not list:
        raise ValueError('lst is type {}'.type(lst))
    ss = 'ANY( VALUES (' + ') ,('.join([str(l) for l in lst]) + '))'
    return ss

def na_none(val):
    vals = str(val)
    if vals == '' or vals == 'None' or vals == 'nan' or vals == 'np.nan':
        return True
    return False

def itemid_summary(itemid, subject = None):
    
    if type(itemid) is int or type(itemid) is float:
        row = to_use_ce[to_use_ce['itemid']==itemid]
    elif type(itemid) is list:
        row = to_use_ce[to_use_ce['itemid']==itemid[0]]
        itemid = any_in_postgres(itemid)
    else:
        raise ValueError('Inappropriate itemid type {}'.format(type(itemid)))
    out_low = row['outlier low'].values[0]
    val_low = row['valid low'].values[0]
    out_high = row['outlier high'].values[0]
    val_high = row['valid high'].values[0]
    if subject is None:
        sub = any_in_postgres(all_subs_lst)
    else:
        sub = '{}'.format(subject)
    string = '''
    select min(valuenum), max(valuenum), avg(valuenum), count(*) cnt
    from chartevents where itemid = {} and subject_id = {};
    '''.format(itemid, sub)
    df1 = pd.read_sql(string, con)
    if df1.loc[0, 'cnt'] == 0:
        df1['min'] = 0.0
        df1['max'] = 0.0
        df1['avg'] = 0.0
    string_low = '''
    select count(*) c from chartevents 
    where itemid = {} and subject_id = {} and valuenum < {};
    '''
    string_high = '''
    select count(*) c from chartevents
    where itemid = {} and subject_id = {} and valuenum > {};
    '''
    if not na_none(out_low):
        has_out_low = 1
        count_out_low = pd.read_sql(string_low.format(itemid, sub, out_low), con)['c'][0]
    else:
        has_out_low = 0
        count_out_low = 0
    
    if not na_none(val_low):
        has_val_low = 1
        count_val_low = pd.read_sql(string_low.format(itemid, sub, val_low), con)['c'][0]
    else:
        has_val_low = 0
        count_val_low = 0
    
    if not na_none(out_low):
        has_out_high = 1
        count_out_high = pd.read_sql(string_high.format(itemid, sub, out_high), con)['c'][0]
    else:
        has_out_high = 0
        count_out_high = 0
    
    if not na_none(val_low):
        has_val_high = 1
        count_val_high = pd.read_sql(string_high.format(itemid, sub, val_high), con)['c'][0]
    else:
        has_val_high = 0
        count_val_high = 0
    
    df1['out_low_per'] = 0.0
    if has_out_low and df1.loc[0, 'cnt']:
        df1['out_low_per'] = count_out_low/df1.loc[:, 'cnt']
        
    df1['val_low_per'] = 0.0
    if has_val_low and df1.loc[0, 'cnt']:
        df1['val_low_per'] = count_val_low/df1.loc[0, 'cnt']
        
    df1['out_high_per'] = 0.0
    if has_out_high and df1.loc[0, 'cnt']:
        df1['out_high_per'] = count_out_high/df1.loc[0, 'cnt']
    
    df1['val_high_per'] = 0.0
    if has_val_high and df1.loc[0, 'cnt']:
        df1['val_high_per'] = count_val_high/df1.loc[0, 'cnt']
    
    return df1

def nitemid_summary(nitemid):
    itemids = to_use_ce['itemid'][to_use_ce['nitemid']==nitemid].tolist()
    return itemid_summary(itemids)

#TODO
# loop through all subjects and add to a df (append to df1)
