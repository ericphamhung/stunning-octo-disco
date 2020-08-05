import pandas as pd
import numpy as np
from math import floor, log10, ceil
from psycopg2 import connect
import variables as vb
from pathlib import Path
import re
from copy import copy
from time import time
con = connect('dbname={} user={} password={}'.format(
    vb.db, vb.sqluser, vb.password))

context  = pd.read_sql(vb.contextselector_temporary, con)
# t0 =time()
# df = pd.read_sql(vb.pivotstring_ie, con)
# df['elapsed'] = 0
# ccols = ['icustay_id', 'nitemid']
# for i, row in df.iterrows():
#     if i % 2 == 0:
#         row0 = copy(row)
#     else:
#         assert all(row[ccols]==row0[ccols])
#         df['elapsed'] = row['time']-row0['time']
# print(time()-t0)

def dummylist(ser):
    dummies=pd.get_dummies(ser)
    dummycols = dummies.columns.tolist()
    return dummies[dummycols[:-1]]

def get_the_dummies(cxt):
    objectmap = context.select_dtypes(include="object").columns.tolist()
#     print(cxt.shape)
    for oo in objectmap:
        tempdf = dummylist(cxt[oo])
        cxt = pd.concat([cxt, tempdf], axis = 1)
        del cxt[oo]
    return cxt

if False:
    string = 'select distinct {} id from timetable;'.format(vb.selectitem)
    idlist =  pd.read_sql(string,con)['id'].unique()
    
    #idlist = df[vb.selectitem].unique()
    idlist = idlist[~np.isnan(idlist)].tolist()
    idlist = [int(i) for i in idlist]
    
    idlist = [i//100 for i in idlist]
    tablelist = [int(str(i)[:1]) for i in idlist]
    cnt = 0
    for i, j in zip(idlist, tablelist):
        if j == 1:
            string = vb.pivotstring_ce.format(i)
        elif j == 2:
            string = vb.pivotstring_ie.format(i)
        elif j == 3:
            string = vb.pivotstring_oe.format(i)
        else:
            print(i, j)
            continue
        tempdf = pd.read_sql(vb.pivotstring.format(i),con)
        
        tempcols = tempdf.columns.tolist()
        common = np.intersect1d(tempcols, context.columns.tolist()).tolist()
        context = context.merge(tempdf,how="left",on=common)
        cnt+=1
        if cnt % 10 == 0:
            print ("{} completed of {}".format(cnt, len(idlist)))
    
    context.to_csv('context_plus_1.csv', index = False)

if False:
    cxt1 = pd.read_sql('select icustay_id, hadm_id from context', con)
    context = pd.read_csv('context_plus_1.csv')
    #print(context.columns)
    context = context.merge(cxt1, on='icustay_id')
    context.to_csv('context_plus_2.csv', index = False)
if True:
#     ids = pd.read_sql('select distinct hadm_id id from context', con)
    context = pd.read_csv('context_plus_2.csv')
    print(context.shape)
    context = get_the_dummies(context)
    print(context.shape)
#     clist = ids['id'].unique().tolist()
    clist = context['hadm_id'].unique().tolist()
    ss = 'ANY( VALUES (' + ') ,('.join([str(u) for u in clist]) + '))'
    dgicd = pd.read_sql('select hadm_id, icd9_code from diagnoses_icd where hadm_id = {};'.format(ss) ,con)
    dgicd = pd.get_dummies(dgicd,columns=['icd9_code'])
    dgicd = pd.pivot_table(dgicd,index= 'hadm_id',
                           values=[col for col in dgicd if col.startswith('icd9')],aggfunc=np.sum)
    dgcols = dgicd.columns.tolist()
    common = np.intersect1d(dgcols, context.columns.tolist()).tolist()
    context = context.merge(dgicd, how="left", on ="hadm_id")
    print(context.shape)
    context.to_csv('context_plus_plus.csv', index = False)
    #         context = context.merge(proicd, how="left", on ="hadm_id")
    #context = context.drop('diagnosis', axis=1)
    #objectmap = context.select_dtypes(include="object").columns.tolist()
    #context = pd.get_dummies(context, columns=objectmap)

