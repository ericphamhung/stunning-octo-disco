import luigi
import psycopg2
import pandas as pd
import datetime
from time import time
from luigi.contrib import postgres
from luigi.contrib import rdbms
import subprocess
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
import variables as vb
import numpy as np
import sklearn
from math import log10, floor

def dummylist(ser, oo):
    dummies=pd.get_dummies(ser, prefix = oo, dummy_na = True)
    dummycols = dummies.columns.tolist()
    return dummies#[dummycols[:-1]]

def get_the_dummies(cxt):
    objectmap = cxt.select_dtypes(include="object").columns.tolist()
    for oo in objectmap:
        tempdf = dummylist(cxt[oo], oo)
        cxt = pd.concat([cxt, tempdf], axis = 1)
        del cxt[oo]
    return cxt


            
            
class AddICD9(luigi.Task):
    dbname=luigi.Parameter(default= vb.targetdb) 
    user=luigi.Parameter(default= vb.sqluser)
    password=luigi.Parameter(default= vb.password)
    host=luigi.Parameter(default= '/var/run/postgresql/')
            
    def output(self):
        return luigi.LocalTarget('/home/ubuntu/analyze_shallow/output/contextwithicd9.csv')


    def run(self):
        con = psycopg2.connect(dbname=self.dbname, 
                               user=self.user, 
                               password=self.password, 
                               host=self.host)
        cursor = con.cursor()            
        
               
                
        context = pd.read_sql('select * from context',con)
        clist = context['hadm_id'].unique().tolist()
        ss = 'ANY( VALUES (' + ') ,('.join([str(u) for u in clist]) + '))'
        
        dtypetime = context.select_dtypes(include="datetime").columns.tolist()
        context = context.drop(dtypetime, axis =1)
        
        dgicd = pd.read_sql('select * from diagnoses_icd where hadm_id = {};'.format(ss) ,con)
        dgicd['d_icd'] = dgicd['icd9_code']
        dgicd = pd.get_dummies(dgicd,columns=['d_icd'])
        dgicd = pd.pivot_table(dgicd,index= 'hadm_id', fill_value = 0,
                               values=[col for col in dgicd if col.startswith('d_')],aggfunc=np.sum)
        
              
        proicd = pd.read_sql('select * from procedures_icd where hadm_id = {};'.format(ss) ,con)
        proicd['p_icd'] = proicd['icd9_code']
        proicd = pd.get_dummies(proicd,columns=['p_icd'])
        proicd = pd.pivot_table(proicd,index= 'hadm_id', fill_value = 0,
                                 values=[col for col in proicd if col.startswith('p_')],aggfunc=np.sum)
        
        
        context = context.merge(dgicd, how="left", on ="hadm_id")
        context = context.merge(proicd, how="left", on ="hadm_id")
        context = context.drop('diagnosis', axis=1)
        context = get_the_dummies(context)
        
        pcols = [col for col in context if col.startswith('p_')]
        dcols = [col for col in context if col.startswith('d_')]
        
        context[pcols]=context[pcols].fillna(0).astype(int)

        
        #TODO add sklearn preprocessing (standardizing all variables)
        
        with self.output().open('w') as f:
            #f.write('completed on {}'.format(datetime.datetime.now()))  
            context.to_csv(f, index=None)

            

class CreateContextPlus(luigi.Task):
    dbname=luigi.Parameter(default= vb.targetdb) 
    user=luigi.Parameter(default= vb.sqluser)
    password=luigi.Parameter(default= vb.password)
    host=luigi.Parameter(default= '/var/run/postgresql/')
    
    def requires(self):
        return AddICD9()
    
    def output(self):
        return luigi.LocalTarget('/home/ubuntu/analyze_shallow/output/contextplus.csv')

    def run(self):
        con = psycopg2.connect(dbname=self.dbname, 
                               user=self.user, 
                               password=self.password, 
                               host=self.host)
        cursor = con.cursor()
        
        context = pd.read_csv("/home/ubuntu/analyze_shallow/output/contextwithicd9.csv")
        #CONTEXT
        #context = pd.read_sql('select * from context;',con)
        #clist = context['icustay_id'].tolist()
        #los_dic = {}
        #for i in clist:
        #    los_dic[i] = ceil(context.loc[context['icustay_id']==i, ['los_total']].values)
        #for i, l in los_dic.items():
        #    tempdf = context[context['icustay_id'] == i]
        #    if l>1:
        #        context = context.append([tempdf]*(l-1), ignore_index=True)
        #    print("completed {}".format(i))
        #TODO create "hours per day" (possibly create new icuday_id)
        
        
        #CHARTEVENTS
        idlist = pd.read_sql('select distinct(cid1, cid2) from chartevents', con)
        for i,row in idlist.iterrows():
            temp = row[0].split('(')[1].split(')')[0].split(',')
            a,b = int(temp[0]), int(temp[1])
            tempdf = pd.read_sql(vb.ce_pivot.format(a,b),con)
            context = context.merge(tempdf,how="left",on='icustay_id')
            print ("{} completed".format(i))
        
        #OUTPUTEVENTS
        idlist = pd.read_sql('select distinct(cid1, cid2) from outputevents', con)
        for i,row in idlist.iterrows():
            temp = row[0].split('(')[1].split(')')[0].split(',')
            a,b = int(temp[0]), int(temp[1])
            tempdf = pd.read_sql(vb.mv_out_pivot.format(a,b,'value','outputevents','out'),con)
            context = context.merge(tempdf,how="left",on='icustay_id')
            print ("{} completed".format(i))

        #INPUT_MV
        idlist = pd.read_sql('select distinct(cid1, cid2) from inputevents_mv', con)
        for i,row in idlist.iterrows():
            temp = row[0].split('(')[1].split(')')[0].split(',')
            a,b = int(temp[0]), int(temp[1])
            tempdf = pd.read_sql(vb.mv_out_pivot.format(a,b,'amount', 'inputevents_mv','in'),con)
            context = context.merge(tempdf,how="left",on='icustay_id')
            print ("{} completed".format(i))       
            


        with self.output().open('w') as f:
            #f.write('completed on {}'.format(datetime.datetime.now()))  
            context.to_csv(f, index=None)

            

            
            
class RunShallow(luigi.WrapperTask):
    def requires(self):
        yield AddICD9()
        yield CreateContextPlus()
            
if __name__ == '__main__':
    luigi.run()