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
import math
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.impute import SimpleImputer
from scipy import stats

###################################################################################
#Prediction will be split into 3 categories:
# 1 Current Clinical Scoring System (SAPS2)
#     we will compare the distribution of the SAPS2 scores
# 2 Prediction Using Logistic Regression
#     we will compare the performance of models on a holdout set
# 3 Prediction Using Deep Neural Nets
#     we compare the perfomance of FeedForward on a holdout
#     we compare the perfomance of Recurrent on a holdout
###################################################################################


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

def getsaps2(dff, idid):
    SAPS_score = 0
    dff=dff[dff['icustay_id']==idid]
    #AGE
    var = 'age_at_adm'
    if dff.iloc[0][var]<40:
        pass
    elif dff.iloc[0][var]<60:
        SAPS_score += 7
    elif dff.iloc[0][var]<70:
        SAPS_score += 12
    elif dff.iloc[0][var]<75:
        SAPS_score += 15
    elif dff.iloc[0][var]<80:
        SAPS_score += 16
    else:
        SAPS_score += 18
    #HEART RATE
    var = 'heart_rate'
    if math.isnan(dff.iloc[0][var]):
        pass
    elif dff.iloc[0][var]<40:
        SAPS_score += 11
    elif dff.iloc[0][var]<70:
        SAPS_score += 2
    elif dff.iloc[0][var]<120:
        SAPS_score += 0
    elif dff.iloc[0][var]<160:
        SAPS_score += 4
    else:
        SAPS_score += 7
    #SYSTOLIC BP
    var = 'systolic_bp'
    if math.isnan(dff.iloc[0][var]):
        pass
    elif dff.iloc[0][var]<70:
        SAPS_score += 13
    elif dff.iloc[0][var]<100:
        SAPS_score += 5
    elif dff.iloc[0][var]<200:
        SAPS_score += 0
    else:
        SAPS_score += 2
    #BODYTEMP
    var = 'body_temp'
    if math.isnan(dff.iloc[0][var]):
        pass
    elif dff.iloc[0][var]<39:
        SAPS_score += 0
    else:
        SAPS_score += 3
    #GCS Total
    var = 'gcs_total'
    if math.isnan(dff.iloc[0][var]):
        pass
    elif dff.iloc[0][var]<6:
        SAPS_score += 26
    elif dff.iloc[0][var]<8:
        SAPS_score += 13
    elif dff.iloc[0][var]<10:
        SAPS_score += 7
    elif dff.iloc[0][var]<14:
        SAPS_score += 5
    else:
        SAPS_score += 0
    #PAO2/FIO2
    var = 'pao2_fio2_ratio'
    if math.isnan(dff.iloc[0][var]):
        pass
    elif dff.iloc[0][var]<100:
        SAPS_score += 11
    elif dff.iloc[0][var]<200:
        SAPS_score += 9
    else:
        SAPS_score += 6
    #BUN
    var = 'bun'
    if math.isnan(dff.iloc[0][var]):
        pass
    elif dff.iloc[0][var]<28:
        SAPS_score += 0
    elif dff.iloc[0][var]<84:
        SAPS_score += 6
    else:
        SAPS_score += 10
    #URINE
    var='urine'
    if math.isnan(dff.iloc[0][var]):
        pass
    elif dff.iloc[0][var]<500:
        SAPS_score += 11
    elif dff.iloc[0][var]<1000:
        SAPS_score += 4
    else:
        SAPS_score += 0
    #SODIUM
    var='sodium'
    if math.isnan(dff.iloc[0][var]):
        pass
    elif dff.iloc[0][var]<125:
        SAPS_score += 5
    elif dff.iloc[0][var]<145:
        SAPS_score += 0
    else:
        SAPS_score += 1
    #POTASSIUM
    var='potassium'
    if math.isnan(dff.iloc[0][var]):
        pass
    elif dff.iloc[0][var]<3:
        SAPS_score += 3
    elif dff.iloc[0][var]<5:
        SAPS_score += 0
    else:
        SAPS_score += 3
    #BICARRBONATE
    var='serum_bicarb'
    if math.isnan(dff.iloc[0][var]):
        pass
    elif dff.iloc[0][var]<15:
        SAPS_score += 6
    elif dff.iloc[0][var]<20:
        SAPS_score += 3
    else:
        SAPS_score += 0
    #BILIRUBIN
    var='bilirubin'
    if math.isnan(dff.iloc[0][var]):
        pass
    elif dff.iloc[0][var]<4:
        SAPS_score += 0
    elif dff.iloc[0][var]<6:
        SAPS_score += 4
    else:
        SAPS_score += 9
    #WBC
    var='wbc'
    if math.isnan(dff.iloc[0][var]):
        pass
    elif dff.iloc[0][var]<1:
        SAPS_score += 12
    elif dff.iloc[0][var]<20:
        SAPS_score += 0
    else:
        SAPS_score += 3
    #CHRONIC DISEASE
    if dff.iloc[0]['has_aids']==1:
        SAPS_score += 17
    elif dff.iloc[0]['has_heme']==1:
        SAPS_score += 10
    elif dff.iloc[0]['has_cancer']==1:
        SAPS_score += 9
    else:
        SAPS_score += 0

    x = -7.7631 + (0.0737 * (SAPS_score)) + (0.9971 * (math.log(SAPS_score +1)))
    mort = math.exp(x)/(1+math.exp(x))

    return mort


class SAPS2Distribution(luigi.Task):
    name = 'SAPS2Distribution'
    def run(self):
        t=time()
        real_saps = pd.DataFrame()
        for i in real_iculist:
            tempdf = pd.DataFrame({'icustay_id':[i], 'SAPS2_mortality':[getsaps2(realdf,i)]})
            real_saps = pd.concat([real_saps,tempdf])
            print('saps2 score for {} calculated!'.format(i))
        # synth_saps = pd.DataFrame()
        # for i in iculist:
        #     tempdf = pd.DataFrame({'icustay_id':[i], 'SAPS2_mortality':[getsaps2(synthdf,i)]})
        #     synth_saps = pd.concat([synth_saps,tempdf])
        #
        # d,p = stats.ks_2samp(real_saps['SAPS2_mortality'],synth_saps['SAPS2_mortality'])
        # print(d,p)
        # with self.output().open('w') as f:
        #     f.write('SAPS2 KS-test result:  p-value = {}'.format(p))

        with self.output().open('w') as f:
            real_saps.to_csv(f,index=None)


    def output(self):
        return luigi.LocalTarget(package_location+'results/{0}.csv'.format(self.name))



class LogisticRegression(luigi.Task):
    name = 'LogisticRegression'
    def run(self):
        t=time()
        # Test
        X_test = realdf_test.drop('died',axis=1).values
        y_test= realdf_test[['died']].values
        
        # Real Train
        X_train = realdf_train.drop('died',axis=1).values
        y_train = realdf_train[['died']].values
        
        # Synthetic 
        Xprime
        yprime
        #real model
        clf_real = LogisticRegression(random_state=0, solver='saga',max_iter=1000000).fit(X_train, y_train)
        
        #synthetic model
        clf_synth = LogisticRegression(random_state=0, solver='saga',max_iter=1000000).fit(Xprime, yprime)
        
        clf_real.score(X_test,y_test)
        clf_synth.score(X_test,y_test)
        
        
        with self.output().open('w') as f:
            f.write('real accuracy = {0}  synthetic accuracy = {1}'.format(clf_real.score(X_test,y_test),clf_synth.score(X_test,y_test)))

    def output(self):
        return luigi.LocalTarget(package_location+'results/{0}.csv'.format(self.name))


class Run_003(luigi.WrapperTask):
    def requires(self):
        yield SAPS2Distribution()
        yield LogisticRegression()


if __name__ == '__main__':
    luigi.run()
