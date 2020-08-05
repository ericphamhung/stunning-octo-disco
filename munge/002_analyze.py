# MUNGE_2.0
#For Shallow Learning


import luigi
import psycopg2
import pandas as pd
import datetime
from luigi.contrib import postgres
from luigi.contrib import rdbms
import subprocess
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
import numpy as np
import os
import csv

from time import time
import seaborn as sns
import math
from scipy import stats


#START HERE
sqluser = 'postgres'
sourcedb = 'mimic'
schema_name = 'mimiciii'
pw = 'postgres'
targetdb = 'munge'
package_location = os.path.dirname(os.path.realpath(__file__))
package_location += '/'
starttime = time()
host = '/var/run/postgresql/'
con = psycopg2.connect(user=sqluser,
                       password=pw,
                       host=host,
                       dbname=targetdb)
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = con.cursor()

#Getting Data

#Getting Train and Test Data

#feature list
fco_list = pd.read_sql("select distinct feature from featureset_a where chart like any(values('chartevents'),('outputevents'))",
                       con)['feature'].tolist()
fl_list = pd.read_sql("select distinct feature from featureset_a where chart like 'labevents'"
                      , con)['feature'].tolist()
fco_list.remove("ignore") 
fl_list.remove("ignore")

#import context
real_train = pd.read_sql('select * from train_context;',con)
real_test = pd.read_sql('select * from test_context;',con)

#import timebucket and daybucket
ss = ''
for f in fco_list:
    ss += 'avg({0}) as {0}, '.format(f)
ss = ss[:-2]

tt = ''
for t in fl_list:
    tt += 'avg({0}) as {0}, '.format(t)
tt = tt[:-2]

uu = ''
for f in fco_list:
    uu += 'avg({0}) as {0}, '.format(f)
uu = uu[:-2]

vv = ''
for t in fl_list:
    vv += 'avg({0}) as {0}, '.format(t)
vv = vv[:-2]

avg_real_1_train = pd.read_sql('select icustay_id, {0} from train_timebucket group by icustay_id;'.format(ss),con)
avg_real_2_train = pd.read_sql('select hadm_id, {0} from train_daybucket group by hadm_id;'.format(tt),con)
avg_real_1_test = pd.read_sql('select icustay_id, {0} from test_timebucket group by icustay_id;'.format(ss),con)
avg_real_2_test = pd.read_sql('select hadm_id, {0} from test_daybucket group by hadm_id;'.format(tt),con)

realdf_train = real_train.merge(avg_real_1_train, how='inner', on='icustay_id')
realdf_train = realdf_train.merge(avg_real_2_train, how='inner', on='hadm_id')
realdf_train['gcs_e'] = realdf_train['gcs_e'].fillna(4)
realdf_train['gcs_v'] = realdf_train['gcs_v'].fillna(5)
realdf_train['gcs_m'] = realdf_train['gcs_m'].fillna(6)
realdf_train['gcs_total'] = realdf_train.gcs_e+realdf_train.gcs_m+realdf_train.gcs_v

realdf_test = real_test.merge(avg_real_1_test, how='inner', on='icustay_id')
realdf_test = realdf_test.merge(avg_real_2_test, how='inner', on='hadm_id')
realdf_test['gcs_e'] = realdf_test['gcs_e'].fillna(4)
realdf_test['gcs_v'] = realdf_test['gcs_v'].fillna(5)
realdf_test['gcs_m'] = realdf_test['gcs_m'].fillna(6)
realdf_test['gcs_total'] = realdf_test.gcs_e+realdf_test.gcs_m+realdf_test.gcs_v

###################################################
#merged training dataset is readldf_train
#merged testing dataset is realdf_test
###################################################

drop_list = ['icustay_id', 'hadm_id', 'subject_id', 'dbsource', 'first_careunit',
       'last_careunit', 'first_wardid', 'last_wardid', 'intime', 'outtime',
       'admittime', 'dischtime', 'deathtime','admission_location', 'discharge_location','edregtime', 'edouttime',
       'diagnosis', 'hospital_expire_flag', 'has_chartevents_data',
       'dob', 'dod', 'dod_hosp', 'dod_ssn', 'expire_flag', 'ser_ind','age_at_death','los_total']

realdf_train = realdf_train.drop(drop_list, axis=1)
realdf_test = realdf_test.drop(drop_list, axis=1)
realdf_train = realdf_train.fillna(realdf_train.median())
realdf_test = realdf_test.fillna(realdf_test.median())

#creates dummies for categorical variables
dummies_list=['admission_type','insurance','language','religion','marital_status','ethnicity']
realdf_train = pd.get_dummies(realdf_train, columns=dummies_list)
realdf_test = pd.get_dummies(realdf_test, columns=dummies_list)

#ensures the test set has the same dummies as the train set
missing_cols = set(realdf_train.columns) - set(realdf_test.columns)
for c in missing_cols:
   realdf_test[c] = 0




# 1. Continuous Variables [Static]
#     KS Test
class StaticContinuous(luigi.Task):
    name = 'StaticContinuous'
    def run(self):
        # get continuous variables
        for f in f_list:
            d,p = stats.ks_2samp(real[f],synthetic[f])


        with self.output().open('w') as f:
            df.to_csv(f,index=None)

    def output(self):
        return luigi.LocalTarget(package_location+'results/{0}.csv'.format(self.name))



# 2. Discrete Variables [Static]
#     Pearson's ChiSquare


# 3. Freq/Count Variables
#     Pearson's ChiSquare


# 4. Continuous Variables [Temporal]
#     Comparison of Mean Vectors?
#     Linear Mixed Effects


# 5. Survival/Time to Event Data
#     Cox Prop Hazard Survival




class Run_002(luigi.WrapperTask):
    def requires(self):
        yield StaticContinuous()


if __name__ == '__main__':
    luigi.run()
