# MUNGE_2.0

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
from sklearn.model_selection import train_test_split


#START HERE
sqluser = 'postgres'
sourcedb = 'mimic'
schema_name = 'mimiciii'
pw = 'postgres'
targetdb = 'munge'
package_location = os.path.dirname(os.path.realpath(__file__))
package_location += '/'
starttime = time()

# from base MIMIC creates a test database
class CreateDb(luigi.Task):

    name='CreateDb'
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        dropdb='DROP DATABASE IF EXISTS {0};'.format(targetdb)
        createdb='CREATE DATABASE {0};'.format(targetdb)
        alterdb='ALTER DATABASE {0} SET search_path=postgres, {1};'.format(targetdb,schema_name)
        restartpsql = 'sudo service postgresql restart'
        importdb='sudo pg_restore -d {0} -j 64 ~/mimic_backup.sqlc -U postgres'.format(targetdb)

        cursor.execute(dropdb)
        con.commit()
        print ('dropped DB')
        cursor.execute(createdb)
        con.commit()
        print ('created DB')
        cursor.execute(alterdb)
        con.commit()
        print ('altered DB')
        subprocess.run(restartpsql, shell=True, check=True)
        print ('restarted postgres')
        subprocess.run(importdb, shell=True, check=False)
        print ('imported DB')

        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def output(self):
        return luigi.LocalTarget(package_location+'output/{0}.txt'.format(self.name))


# getting admission_ids from icustays and transfers
class GetAdm(luigi.Task):
    name = 'GetAdm'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()


        cursor.execute('drop table if exists admission_ids;')
        con.commit()

        #you can refine your cohort selection criteria here
        cursor.execute('''
        create table if not exists admission_ids
        as (select hadm_id from services where curr_service='CMED' or curr_service='CSURG'
        intersect
        select hadm_id from icustays
        );
        ''')
        con.commit()

        #removing patients greater than 90years old and getting first admission
        add_this_to_subset = '''
        intersect
        select hadm_id
        from admission_ids
        where hadm_id in (select distinct on (subject_id) hadm_id
                       from (select * from admissions order by admittime) tt);
        '''
        creteadm_string = '''
        with foo as (
            select a.subject_id, a.hadm_id, extract(epoch from a.admittime - p.dob)/60/60/24/365.242 as age_at_adm
            from patients p
            inner join admissions a
            on p.subject_id = a.subject_id
            )
        select hadm_id
        from foo where age_at_adm < 90
        ;
        '''
        df = pd.read_sql(creteadm_string, con)

        with self.output().open('w') as f:
            df.to_csv(f,index=None)


    def requires(self):
        return CreateDb()

    def output(self):
        return luigi.LocalTarget(package_location+'output/{0}.csv'.format(self.name))


class CreateContextWithTimeFix(luigi.Task):
    name = 'CreateContextWithTimeFix'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        cursor.execute('drop table if exists context cascade;')
        con.commit()
        cursor.execute('drop table if exists temp_context cascade;')
        con.commit()

        #selecting hadm_id
        admission_ids = pd.read_csv(package_location+'output/GetAdm.csv')['hadm_id'].tolist()
        adm_string = 'ANY( VALUES (' + ') ,('.join([str(u) for u in admission_ids]) + '))'

        #creating context as a union of patients, admissions, and icustays

        context_string= '''
        CREATE TABLE temp_context as (
            WITH foo AS(
            SELECT   icustay_id, icu.hadm_id, icu.subject_id, icu.dbsource, icu.first_careunit, icu.last_careunit
            , icu.first_wardid, icu.last_wardid, icu.intime, icu.outtime

            , adm.admittime, adm.dischtime, adm.deathtime, adm.admission_type, adm.admission_location
            , adm.discharge_location, adm.insurance, adm.language, adm.religion, adm.marital_status, adm.ethnicity
            , adm.edregtime, adm.edouttime, adm.diagnosis, adm.hospital_expire_flag, has_chartevents_data

            , pat.gender, pat.dob, pat.dod, pat.dod_hosp, pat.dod_ssn, pat.expire_flag

            FROM admissions adm
            LEFT JOIN patients pat ON pat.subject_id = adm.subject_id
            LEFT JOIN icustays icu ON icu.hadm_id = adm.hadm_id
            LEFT JOIN services sev ON sev.hadm_id = adm.hadm_id
            WHERE adm.hadm_id = {}),

            bar AS(
            SELECT hadm_id, sum(service_ind) ser_ind from (
                    SELECT hadm_id, CASE WHEN curr_service = 'CMED' then 1
                                         WHEN curr_service = 'CSURG' then 2
                                         ELSE 0 END as service_ind
                    FROM services
                ) foo where foo.service_ind > 0 group by hadm_id
                )

            SELECT  icustay_id, foo.hadm_id, subject_id, dbsource, first_careunit, last_careunit
                    , first_wardid, last_wardid, intime, outtime

                    , admittime, dischtime, deathtime, admission_type, admission_location
                    , discharge_location, insurance, language, religion, marital_status, ethnicity
                    , edregtime, edouttime, diagnosis, hospital_expire_flag, has_chartevents_data

                    , gender, dob, dod, dod_hosp, dod_ssn, expire_flag

                    , ser_ind

                    , EXTRACT(EPOCH FROM dod - dob)/60.0/60.0/24.0/365.242 as age_at_death
                    , EXTRACT(EPOCH FROM admittime - dob)/60.0/60.0/24.0/365.242 as age_at_adm
                    , EXTRACT(EPOCH FROM outtime - intime)/60.0/60.0/24.0 as los_total
                    , CASE WHEN dod IS NOT NULL then 1 ELSE 0 END as died
                    , CASE WHEN dod - dischtime < INTERVAL '30 days' then 1 ELSE 0 END as died_inhouse

            FROM foo
            LEFT JOIN bar ON foo.hadm_id = bar.hadm_id
        )
        ;

        CREATE TABLE context AS(
        SELECT DISTINCT ON (icustay_id) * ,
            DENSE_RANK() OVER (PARTITION BY hadm_id ORDER BY intime) as icu_rank,
            DENSE_RANK() OVER (PARTITION BY subject_id ORDER BY admittime) as adm_rank
        FROM temp_context
        );

        '''

        cursor.execute(context_string.format(adm_string))

        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))


    def requires(self):
        return GetAdm()
    def output(self):
        return luigi.LocalTarget(package_location+'output/{0}.txt'.format(self.name))


class ItemIdList(luigi.Task):
    name = 'ItemIdList'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        #selecting hadm_id
        admission_ids = pd.read_csv(package_location+'output/GetAdm.csv')['hadm_id'].tolist()
        adm_string = 'ANY( VALUES (' + ') ,('.join([str(u) for u in admission_ids]) + '))'


        #getting itemids from different tables
        input_itemid = pd.read_sql('''
        with inputitemids as (
        select distinct itemid, hadm_id from inputevents_mv where itemid >= 200000
        union
        select distinct itemid, hadm_id from inputevents_cv where itemid >= 30000 and itemid <= 49999
            ) select distinct itemid from inputitemids where hadm_id = {0};
        '''.format(adm_string), con)['itemid'].tolist()
        output_itemid =  pd.read_sql('''
        select distinct itemid from outputevents where hadm_id = {0};
        '''.format(adm_string), con)['itemid'].tolist()
        chart_itemid = pd.read_sql('''
        select distinct itemid from chartevents where hadm_id = {0};
        '''.format(adm_string), con)['itemid'].tolist()
        lab_itemid = pd.read_sql('''
        select distinct itemid from labevents where hadm_id = {0};
        '''.format(adm_string), con)['itemid'].tolist()
        prescript_itemid = pd.read_sql('''
        select distinct formulary_drug_cd from prescriptions where hadm_id = {0};
        '''.format(adm_string), con)['formulary_drug_cd'].tolist()
        sql = '''select distinct (spec_itemid,org_itemid,ab_itemid),spec_itemid,org_itemid,ab_itemid
              from microbiologyevents where hadm_id = {0};'''.format(adm_string)
        cursor.execute(sql)
        res = cursor.fetchall()
        microbio_itemid = []
        for r in res:
            ele = r[0][1:-1].split(',')
            for t in range(len(ele)):
                try:
                    ele[t] = int(ele[t])
                except:
                    ele[t] = None
            microbio_itemid.append(tuple(ele))

        database = {'input': input_itemid,
                    'output': output_itemid,
                    'chart': chart_itemid,
                    'lab': lab_itemid,
                    'microbio': microbio_itemid,
                    'prescript': prescript_itemid}
        np.save('output/itemids.npy', database)


        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        return GetAdm()

    def output(self):
        return luigi.LocalTarget(package_location+'output/{0}.txt'.format(self.name))


class AddServices(luigi.Task):
    name = 'AddServices'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con = psycopg2.connect(user=self.user,
                               password=self.password,
                               host=self.host,
                               dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        #get unique list of curr_service
        serv_string = 'SELECT DISTINCT curr_service from services'
        service_list = pd.read_sql(serv_string, con)['curr_service'].tolist()

        #create temp serv table
        serv_string1 = '''
        drop table if exists serv cascade;
        create table serv as (select distinct hadm_id from context);
        '''
        cursor.execute(serv_string1)
        con.commit()

        #adding the ser columns to context table
        serv_string2 = """
        ALTER TABLE serv DROP COLUMN IF EXISTS {0};
        ALTER TABLE serv ADD COLUMN {0} SMALLINT;
        drop table if exists t_{0} cascade;
        create table t_{0} as (
            select distinct hadm_id, hadm_id/hadm_id as {0}
            from services where curr_service = '{0}'
            );
        update serv
            set {0} = s.{0}
            from t_{0} s
            where s.hadm_id = serv.hadm_id;

        ALTER TABLE context DROP COLUMN IF EXISTS {0};
        ALTER TABLE context ADD COLUMN {0} SMALLINT;

        UPDATE context set {0} = s.{0}
        from serv s
        where s.hadm_id = context.hadm_id;
        UPDATE context set {0} = 0
        WHERE {0} IS NULL;

        """
        for s in service_list:
            cursor.execute(serv_string2.format(s))
            con.commit()
            print('service added : {}'.format(s))

        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))


    def requires(self):
        return CreateContextWithTimeFix()

    def output(self):
        return luigi.LocalTarget(package_location+'output/{0}.txt'.format(self.name))


#I've created this to tailor specifically to the SAPSII score system
class ItemIdList2(luigi.Task):
    name = 'ItemIdList2'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con = psycopg2.connect(user=self.user,
                               password=self.password,
                               host=self.host,
                               dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        # selecting hadm_id
        admission_ids = pd.read_sql('select distinct hadm_id from context where hadm_id is not null',con)['hadm_id'].tolist()
        adm_string = 'ANY( VALUES (' + ') ,('.join([str(u) for u in admission_ids]) + '))'

        #uploading feature set A curated from mimic benchmark paper
        cursor.execute('drop table if exists featureset_a cascade;')
        con.commit()
        engine = create_engine('postgresql+psycopg2://{0}:{1}@localhost/{2}'.format(self.user,
                                                                                    self.password,
                                                                                    self.dbname))
        featureset_a = pd.read_csv(package_location+'featureset_a.csv')
        featureset_a.to_sql('featureset_a', engine)

        sql_string = "select itemid from featureset_a where chart = '{}'";

        output_itemid = pd.read_sql(sql_string.format("outputevents"),con)['itemid'].tolist()
        chart_itemid = pd.read_sql(sql_string.format("chartevents"),con)['itemid'].tolist()
        lab_itemid = pd.read_sql(sql_string.format("labevents"),con)['itemid'].tolist()

        database = {'output': output_itemid,
                    'chart': chart_itemid,
                    'lab': lab_itemid,
                    }

        np.save('output/itemids.npy', database)

        getdistinctfeature_string = """
        select distinct feature from featureset_a
        """
        df = pd.read_sql(getdistinctfeature_string,con)

        with self.output().open('w') as f:
            df.to_csv(f,index=None)

    def requires(self):
        yield GetAdm()
        yield CreateContextWithTimeFix()

    def output(self):
        return luigi.LocalTarget(package_location+'output/{0}.csv'.format(self.name))



class FilterDiagnoses(luigi.Task):
    name = 'FilterDiagnoses'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con = psycopg2.connect(user=self.user,
                               password=self.password,
                               host=self.host,
                               dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        ##CANCER
        cancer_string = """
        select icd9_code from d_icd_diagnoses where icd9_code like any(values
        ('14%'),('15%'),('16%'), ('18%'), ('19%'), ('20%'), ('23%'),
        ('170%'), ('171%'), ('172%'), ('173%'), ('174%'), ('175%'), ('179%')
        );
        """
        cancerlist = pd.read_sql(cancer_string,con)['icd9_code'].tolist()
        cancerlist_string = "ANY( VALUES ('" + "') ,('".join([str(a) for a in cancerlist]) + "'))"

        ##AIDS (HIV infection)
        aids_string = """
        select icd9_code from d_icd_diagnoses where icd9_code like '042'
        """
        aidslist = pd.read_sql(aids_string,con)['icd9_code'].tolist()
        aidslist_string = "ANY( VALUES ('" + "') ,('".join([str(b) for b in aidslist]) + "'))"


        ##HEMATOLOGIC MALIGNANCY (includes lymphoma, acute leukemia(2040, 2050), or multiple myeloma (2030,2031)
        hemo_string = """
        select icd9_code from d_icd_diagnoses where icd9_code like any(values
        ('2030%'),('2031%'),('2040%'),('2050%'), ('2020%'), ('2022%'), ('2024%'), ('2026%'), ('2027%'), ('2028%')
        );
        """
        hemelist = pd.read_sql(hemo_string,con)['icd9_code'].tolist()
        hemelist_string = "ANY( VALUES ('" + "') ,('".join([str(c) for c in hemelist]) + "'))"

        diagno_string = """
        drop table if exists diagnoses cascade;
        create table diagnoses as (select distinct hadm_id from context);
        alter table diagnoses
            add column has_cancer smallint,
            add column has_aids smallint,
            add column has_heme smallint;

        drop table if exists cancer cascade;
        create table cancer as (
            select distinct hadm_id, hadm_id/hadm_id as has_cancer
                                from diagnoses_icd where icd9_code = {0}
                                );
        update diagnoses
            set has_cancer = c.has_cancer
            from cancer c
            where c.hadm_id = diagnoses.hadm_id;


        drop table if exists aids cascade;
        create table aids as (
            select distinct hadm_id, hadm_id/hadm_id as has_aids
                                from diagnoses_icd where icd9_code = {1}
                                );
        update diagnoses
            set has_aids = c.has_aids
            from aids c
            where c.hadm_id = diagnoses.hadm_id;


        drop table if exists heme cascade;
        create table heme as (
            select distinct hadm_id, hadm_id/hadm_id as has_heme
                                from diagnoses_icd where icd9_code = {2}
                                );
        update diagnoses
            set has_heme = c.has_heme
            from heme c
            where c.hadm_id = diagnoses.hadm_id;



        """.format(cancerlist_string,aidslist_string,hemelist_string)
        # print(diagno_string)
        cursor.execute(diagno_string)
        con.commit()

        #TODO FIX THIS TO MAKE IT GENERALIZEABLE
        combine_context = """
        ALTER TABLE context drop column if exists has_cancer;
        ALTER TABLE context drop column if exists has_aids;
        ALTER TABLE context drop column if exists has_heme;
        ALTER TABLE context add column has_cancer smallint;
        ALTER TABLE context add column has_aids smallint;
        ALTER TABLE context add column has_heme smallint;

        UPDATE context set has_cancer = d.has_cancer
        from diagnoses d
        where d.hadm_id = context.hadm_id;
        UPDATE context set has_cancer = 0
        WHERE has_cancer IS NULL;

        UPDATE context set has_aids = d.has_aids
        from diagnoses d
        where d.hadm_id = context.hadm_id;
        UPDATE context set has_aids = 0
        WHERE has_aids IS NULL;


        UPDATE context set has_heme = d.has_heme
        from diagnoses d
        where d.hadm_id = context.hadm_id;
        UPDATE context set has_heme = 0
        WHERE has_heme IS NULL;

        """
        cursor.execute(combine_context)
        con.commit()

        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        return CreateContextWithTimeFix()

    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))


#Selecting most frequent unit of measurement for a given itemid in both inputcv and input mv
class FilterInputItemId(luigi.Task):
    name = 'FilterInputItemId'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        #selecting hadm_id
        admission_ids = pd.read_csv(package_location+'output/GetAdm.csv')['hadm_id'].tolist()

        adm_string = 'ANY( VALUES (' + ') ,('.join([str(u) for u in admission_ids]) + '))'
        db = np.load('output/itemids.npy').tolist()
        input_itemid = db['input']

        sql1 = '''
        DROP TABLE IF EXISTS inputcv CASCADE;
        DROP TABLE IF EXISTS inputmv CASCADE;
        CREATE TABLE inputcv as (SELECT * FROM inputevents_cv WHERE hadm_id = {0});
        CREATE TABLE inputmv as (SELECT * FROM inputevents_mv WHERE hadm_id = {0});
        '''.format(adm_string)
        cursor.execute(sql1)
        con.commit()
        print('created inputcv and inputmv')

        #this code returns the unit of measurement(uom) and the number of observations associated with that uom
        input_sql = """select amountuom, sum(count::int) from (select coalesce(amountuom, \'\') as amountuom,
        count(*) from inputevents_cv where itemid = {0} and hadm_id in (select * from admission_ids)
        group by amountuom union all select coalesce(amountuom, \'\') as amountuom, count(*) from inputevents_mv
        where itemid = {0} and hadm_id in (select * from admission_ids) group by amountuom) as t
        where t.amountuom <> \'\' group by t.amountuom;"""

        valid_vurepairs = {}
        for i in input_itemid:
            t=time()
            try:
                tempdf = pd.read_sql(input_sql.format(i),con)
                if len(tempdf)>1:
                    tempdf = tempdf.sort_values(by=['sum'], ascending=False)
                    valid_vurepairs['itemid']='amountuom'
                    valid_vurepairs[i] = tempdf['amountuom'].iloc[0]
                    print('{0} has multiple UOMs'.format(i))
                else:
                    pass
            except:
                print('ommitted itemid {0} due to error'.format(i))

            with self.output().open('w') as f:
                w = csv.writer(f)
                for key, value in valid_vurepairs.items():
                    w.writerow([key,value])

    def requires(self):
        return ItemIdList()

    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.csv'.format(self.name))

class UploadValidUOM(luigi.Task):
    name = 'UploadValidUOM'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con = psycopg2.connect(user=self.user,
                               password=self.password,
                               host=self.host,
                               dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        engine = create_engine('postgresql+psycopg2://{0}:{1}@localhost/{2}'.format(self.user,
                                                                                    self.password,
                                                                                    self.dbname))
        validuom = pd.read_csv(package_location + 'output/FilterInputItemId.csv')
        cursor.execute('DROP TABLE IF EXISTS validuom;')
        con.commit()
        validuom.to_sql('validuom',engine)

        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        return FilterInputItemId()

    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))




class FilterInputCV(luigi.Task):
    name = 'FilterInputCV'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con = psycopg2.connect(user=self.user,
                               password=self.password,
                               host=self.host,
                               dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        cvstring = '''
        SELECT itemid, amountuom FROM validuom;
        '''

        df = pd.read_sql(cvstring,con)


        #create a subset of inputcv  keeping consistent units
        #SET NEW VARIABLE TODELETE (THIS MAY BE MUCH MORE EFFICIENT)
        createtodelete_string = '''
        ALTER TABLE input{0} DROP COLUMN IF EXISTS todelete;
        ALTER TABLE input{0}
            ADD COLUMN todelete SMALLINT;
            '''
        settodelete_string = '''
        UPDATE input{0}
            SET todelete =
                CASE
                    WHEN itemid = {1} AND amountuom != '{2}' THEN 1
                END;
            '''
        cursor.execute(createtodelete_string.format('cv'))
        con.commit()

        for index, row in df.iterrows():
            t=time()
            cursor.execute(settodelete_string.format('cv',row['itemid'],row['amountuom']))
            con.commit()
            print('CV: {0} took {1} seconds'.format(row,time()-t))

        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        return UploadValidUOM()

    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))


class FilterInputMV(luigi.Task):
    name = 'FilterInputMV'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con = psycopg2.connect(user=self.user,
                               password=self.password,
                               host=self.host,
                               dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        cvstring = '''
        SELECT itemid, amountuom FROM validuom;
        '''

        df = pd.read_sql(cvstring, con)

        # create a subset of inputmv  keeping consistent units
        # SET NEW VARIABLE TODELETE (THIS MAY BE MUCH MORE EFFICIENT)
        createtodelete_string = '''
        ALTER TABLE input{0} DROP COLUMN IF EXISTS todelete;
        ALTER TABLE input{0}
            ADD COLUMN todelete SMALLINT;
            '''
        settodelete_string = '''
        UPDATE input{0}
            SET todelete =
                CASE
                    WHEN itemid = {1} AND amountuom != '{2}' THEN 1
                END;
            '''
        cursor.execute(createtodelete_string.format('mv'))
        con.commit()

        for index, row in df.iterrows():
            t = time()
            cursor.execute(settodelete_string.format('mv', row['itemid'], row['amountuom']))
            con.commit()
            print('MV: {0} took {1} seconds'.format(row, time() - t))

        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        return UploadValidUOM()

    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))



class FilterOutputItemId(luigi.Task):
    name = 'FilterOutputItemId'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        chart = 'output'

        #selecting admissions and itemids
        admission_ids = pd.read_sql('select distinct hadm_id from context where hadm_id is not null',con)['hadm_id'].tolist()
        adm_string = 'ANY( VALUES (' + ') ,('.join([str(u) for u in admission_ids]) + '))'
        db = np.load('output/itemids.npy').tolist()
        item_list = db[chart]
        chart_string = 'ANY( VALUES (' + ') ,('.join([str(i) for i in item_list]) + '))'


        sql1 = '''
        DROP TABLE IF EXISTS {2} CASCADE;
        CREATE TABLE {2} as (SELECT * FROM {2}events WHERE hadm_id = {0}
                                INTERSECT
                                SELECT * FROM {2}events WHERE itemid = {1});
        '''.format(adm_string,chart_string,chart)
        cursor.execute(sql1)
        con.commit()

        #DOING THIS FOR CONSISTANCY WITH OTHER TABLES
        sql2='''
        ALTER TABLE output ADD COLUMN valuenum real;
        UPDATE output SET valuenum = value;
        '''
        cursor.execute(sql2)
        con.commit()

        #Outputevents itemids don't need fixing (all in ml)
        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        return ItemIdList2()
    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))


class FilterChartItemId(luigi.Task):
    name = 'FilterChartItemId'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        chart = 'chart'

        #selecting admissions and itemids
        admission_ids = pd.read_sql('select distinct hadm_id from context where hadm_id is not null',con)['hadm_id'].tolist()
        adm_string = 'ANY( VALUES (' + ') ,('.join([str(u) for u in admission_ids]) + '))'
        db = np.load('output/itemids.npy').tolist()
        item_list = db[chart]
        chart_string = 'ANY( VALUES (' + ') ,('.join([str(i) for i in item_list]) + '))'


        sql1 = '''
        DROP TABLE IF EXISTS {2} CASCADE;
        CREATE TABLE {2} as (SELECT * FROM {2}events WHERE hadm_id = {0}
                                INTERSECT
                                SELECT * FROM {2}events WHERE itemid = {1});
        '''.format(adm_string,chart_string,chart)
        cursor.execute(sql1)
        con.commit()

        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        return ItemIdList2()
    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))


class FilterLabItemId(luigi.Task):
    name = 'FilterLabItemId'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        chart = 'lab'

        #selecting admissions and itemids
        admission_ids = pd.read_sql('select distinct hadm_id from context where hadm_id is not null',con)['hadm_id'].tolist()
        adm_string = 'ANY( VALUES (' + ') ,('.join([str(u) for u in admission_ids]) + '))'
        db = np.load('output/itemids.npy').tolist()
        item_list = db[chart]
        chart_string = 'ANY( VALUES (' + ') ,('.join([str(i) for i in item_list]) + '))'


        sql1 = '''
        DROP TABLE IF EXISTS {2} CASCADE;
        CREATE TABLE {2} as (SELECT * FROM {2}events WHERE hadm_id = {0}
                                INTERSECT
                                SELECT * FROM {2}events WHERE itemid = {1});
        '''.format(adm_string,chart_string,chart)
        cursor.execute(sql1)
        con.commit()


        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        return ItemIdList2()
    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))


class FilterMicrobioItemId(luigi.Task):
    name = 'FilterMicrobioItemId'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        return ItemIdList()
    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))


class FilterPrescriptItemId(luigi.Task):
    name = 'FilterPrescriptItemId'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        return ItemIdList()
    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))


class FixChartItemId(luigi.Task):
    name = 'FixChartItemId'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()
        chart = 'chart'
        db = np.load('output/itemids.npy').tolist()
        item_list = db[chart]

        addfeature_string ="""
        alter table {0} drop column if exists feature;
        alter table {0} add column feature text;
        alter table {0} drop column if exists t_since;
        alter table {0} add column t_since real;
        alter table {0} drop column if exists t_til;
        alter table {0} add column t_til real;

        update {0}
            set t_since = extract(epoch from charttime - c.admittime)/60.0/60.0
            from context c
            where c.hadm_id = {0}.hadm_id;

        update {0}
            set t_til = extract (epoch from c.dischtime - charttime)/60.0/60.0
            from context c
            where c.hadm_id = {0}.hadm_id;

        update {0}
            set feature = f.feature
            from featureset_a f
            where f.itemid = {0}.itemid;
        """.format(chart)

        cursor.execute(addfeature_string)
        con.commit()

        farentocelc_fix = """
        UPDATE chart
        set valuenum = (valuenum::real - 32)*5/9
        where itemid = any(values(678),(223761));
        """
        cursor.execute(farentocelc_fix)
        con.commit()

        #temperature outlier removal (might want to do this for all variables?)
        outlier_string = """
        with bounds as (
            select  (avg(valuenum) - stddev_samp(valuenum)*3) as lower_bound,
                    (avg(valuenum) + stddev_samp(valuenum)*3) as upper_bound
            from chart
            where itemid = any(values(678),(223761),(676),(223762))
        )
        delete from chart
        where valuenum not between (select lower_bound from bounds) and (select upper_bound from bounds)
        and itemid = any(values(678),(223761),(676),(223762));
        """
        cursor.execute(outlier_string)
        con.commit()

        #fixing silly outliers that make no sense
        remove_silly = """
        DELETE FROM chart
        WHERE valuenum = 0
        AND itemid = any(values(51),(442),(455),(6701),(220179),(220050));

        DELETE FROM chart
        WHERE valuenum > 400
        AND itemid = any(values(51),(442),(455),(6701),(220179),(220050));

        DELETE FROM chart
        WHERE valuenum > 300
        AND itemid = any(values(211),(220045));

        DELETE FROM chart
        WHERE valuenum = 0
        AND itemid = 646;



        """
        cursor.execute(remove_silly)
        con.commit()


        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        return FilterChartItemId()
    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))



class FixOutputItemId(luigi.Task):
    name = 'FixOutputItemId'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()


        chart = 'output'

        db = np.load('output/itemids.npy').tolist()
        item_list = db[chart]

        addfeature_string ="""
        alter table {0} drop column if exists feature;
        alter table {0} add column feature text;
        alter table {0} drop column if exists t_since;
        alter table {0} add column t_since real;
        alter table {0} drop column if exists t_til;
        alter table {0} add column t_til real;

        update {0}
            set t_since = extract(epoch from charttime - c.admittime)/60.0/60.0
            from context c
            where c.hadm_id = {0}.hadm_id;

        update {0}
            set t_til = extract (epoch from c.dischtime - charttime)/60.0/60.0
            from context c
            where c.hadm_id = {0}.hadm_id;

        update {0}
            set feature = f.feature
            from featureset_a f
            where f.itemid = {0}.itemid;
        """.format(chart)

        cursor.execute(addfeature_string)
        con.commit()

        #outlier
        outlier_string = """
        DELETE FROM output
        WHERE valuenum > 1500
        """

        cursor.execute(outlier_string)
        con.commit()

        #nothing more needs to be done since all table is measured in ml

        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        return FilterOutputItemId()
    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))



class FixLabItemId(luigi.Task):
    name = 'FixLabItemId'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()


        chart = 'lab'

        db = np.load('output/itemids.npy').tolist()
        item_list = db[chart]

        addfeature_string ="""
        alter table {0} drop column if exists feature;
        alter table {0} add column feature text;
        alter table {0} drop column if exists t_since;
        alter table {0} add column t_since real;
        alter table {0} drop column if exists t_til;
        alter table {0} add column t_til real;

        update {0}
            set t_since = extract(epoch from charttime - c.admittime)/60.0/60.0
            from context c
            where c.hadm_id = {0}.hadm_id;

        update {0}
            set t_til = extract (epoch from c.dischtime - charttime)/60.0/60.0
            from context c
            where c.hadm_id = {0}.hadm_id;

        update {0}
            set feature = f.feature
            from featureset_a f
            where f.itemid = {0}.itemid;

        """.format(chart)

        cursor.execute(addfeature_string)
        con.commit()

        #Outlier removal
        outlier_string = """
        DELETE FROM lab
        WHERE valuenum > 40
        AND itemid = 50912;

        DELETE FROM lab
        WHERE valuenum > 50
        AND itemid = any(values(51300),(51301));

        UPDATE lab
        SET valuenum = valuenum*0.05847953216374269
        WHERE itemid = 50885
        AND valuenum > 3;
        """
        cursor.execute(outlier_string)
        con.commit()

        #nothing more needs to be done since all table is measured in ml

        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        return FilterLabItemId()
    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))



class CreateTimeTable(luigi.Task):
    name = 'CreateTimeTable'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        ######merging temporal tables######


        createtime_string = '''
        DROP TABLE IF EXISTS temptimetable CASCADE;
        CREATE TABLE temptimetable (
        hadm_id integer,
        icustay_id integer,
        time timestamp,
        t_since real,
        t_til real
        );

        '''
        cursor.execute(createtime_string)
        con.commit()


        #gets the feature list and adds them into timetable
        feature_list = pd.read_csv(package_location+'/output/ItemIdList2.csv')['feature'].tolist()
        featurecolumns_string = '''
        ALTER TABLE temptimetable
        ADD COLUMN {} real;
        '''
        for i in feature_list:
            cursor.execute(featurecolumns_string.format(i))
            con.commit()
            print('created column {}'.format(i))

        #ICU
        #gets distinct feature from each table and puts into a dict
        icu_tables = ['output','chart']
        getfeaturebychart_string = """
        select distinct feature from featureset_a where chart = '{}events'
        """
        icu_feature_dictionary = {}
        for t in icu_tables:
            var = pd.read_sql(getfeaturebychart_string.format(t),con)['feature'].tolist()
            icu_feature_dictionary[t] = var
            print("completed table {}".format(t))

        icu_includetables_string = """
        INSERT INTO temptimetable (hadm_id, icustay_id, time, t_since, t_til, {0})
        SELECT hadm_id, icustay_id, charttime, t_since, t_til, valuenum::real
        FROM
            (SELECT DISTINCT ON (hadm_id, t_since, feature) *
        FROM {1} WHERE feature = '{0}') as foo
        ;
        """
        for key, value in icu_feature_dictionary.items():
            for v in value:
                cursor.execute(icu_includetables_string.format(v,key))
                con.commit()

        #ADM
        adm_tables = ['lab']
        getfeaturebychart_string = """
        select distinct feature from featureset_a where chart = '{}events'
        """
        adm_feature_dictionary = {}
        for t in adm_tables:
            var = pd.read_sql(getfeaturebychart_string.format(t),con)['feature'].tolist()
            adm_feature_dictionary[t] = var
            print("completed table {}".format(t))


        #iterates through the feature dictionary to insert into timetable
        adm_includetables_string = """
        INSERT INTO temptimetable (hadm_id, time, t_since, t_til, {0})
        SELECT hadm_id, charttime, t_since, t_til, valuenum::real
        FROM
            (SELECT DISTINCT ON (hadm_id, t_since, feature) *
        FROM {1} WHERE feature = '{0}') as foo
        ;
        """
        for key, value in adm_feature_dictionary.items():
            for v in value:
                cursor.execute(adm_includetables_string.format(v,key))
                con.commit()



        #pivot
        timetable_final ="""
        DROP TABLE IF EXISTS timetable CASCADE;
        CREATE TABLE timetable as (
        SELECT hadm_id, icustay_id, time, t_since, t_til, {}
        FROM temptimetable
        GROUP BY hadm_id, icustay_id, t_since, t_til, time
        ORDER BY hadm_id, t_since);
        """
        gg = ''
        for a in feature_list:
            gg += 'sum({0}) as {0},'.format(a)
        gg = gg[:-1]

        cursor.execute(timetable_final.format(gg))
        con.commit()

        #removing negative t_tils
        remove_neg_times = """
        DELETE FROM timetable
        WHERE t_til<0;
        DELETE FROM timetable
        WHERE t_since<0;
        """
        cursor.execute(remove_neg_times)
        con.commit()


        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        yield FixOutputItemId()
        yield FixChartItemId()

    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))

class CreateDaily(luigi.Task):
    name = 'CreateDaily'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        ######merging temporal tables######


        createtime_string = '''
        DROP TABLE IF EXISTS tempdaily CASCADE;
        CREATE TABLE tempdaily (
        hadm_id integer,
        icustay_id integer,
        time timestamp,
        t_since real,
        t_til real
        );

        '''
        cursor.execute(createtime_string)
        con.commit()


        #gets the feature list and adds them into timetable
        feature_list = pd.read_csv(package_location+'/output/ItemIdList2.csv')['feature'].tolist()
        featurecolumns_string = '''
        ALTER TABLE tempdaily
        ADD COLUMN {} real;
        '''
        for i in feature_list:
            cursor.execute(featurecolumns_string.format(i))
            con.commit()
            print('created column {}'.format(i))

        #gets distinct feature from each table and puts into a dict
        adm_tables = ['lab']
        getfeaturebychart_string = """
        select distinct feature from featureset_a where chart = '{}events'
        """
        adm_feature_dictionary = {}
        for t in adm_tables:
            var = pd.read_sql(getfeaturebychart_string.format(t),con)['feature'].tolist()
            adm_feature_dictionary[t] = var
            print("completed table {}".format(t))


        #iterates through the feature dictionary to insert into timetable
        adm_includetables_string = """
        INSERT INTO tempdaily (hadm_id, time, t_since, t_til, {0})
        SELECT hadm_id, charttime, t_since, t_til, valuenum::real
        FROM
            (SELECT DISTINCT ON (hadm_id, t_since, feature) *
        FROM {1} WHERE feature = '{0}') as foo
        ;
        """
        for key, value in adm_feature_dictionary.items():
            for v in value:
                cursor.execute(adm_includetables_string.format(v,key))
                con.commit()

        #pivot
        timetable_final ="""
        DROP TABLE IF EXISTS daily CASCADE;
        CREATE TABLE daily as (
        SELECT hadm_id, icustay_id, time, t_since, t_til, {}
        FROM tempdaily
        GROUP BY hadm_id, icustay_id, t_since, t_til, time
        ORDER BY hadm_id, t_since);
        """
        gg = ''
        for a in feature_list:
            gg += 'sum({0}) as {0},'.format(a)
        gg = gg[:-1]

        cursor.execute(timetable_final.format(gg))
        con.commit()

        #removing negative t_tils
        remove_neg_times = """
        DELETE FROM daily
        WHERE t_til<0;
        DELETE FROM daily
        WHERE t_since<0;
        """
        cursor.execute(remove_neg_times)
        con.commit()


        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        yield FixLabItemId()

    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))


class CreateTimeBucket(luigi.Task):
    name = 'CreateTimeBucket'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        ######Creating time buckets######
        # if anything happens between 4 and 430 it gets looped to 4 ~ JQ
        bucket_string = """
        drop table if exists timebucket cascade;
        create table timebucket as (
            select timestamp 'epoch' + interval '1 second' * round(extract('epoch' from time)/1800)*1800
            as time, hadm_id, icustay_id,
            avg(gcs_v) as gcs_v,
            avg(gcs_e) as gcs_e,
            avg(gcs_m) as gcs_m,
            avg(heart_rate) as heart_rate,
            avg(sp02) as sp02,
            avg(resp_rate) as resp_rate,
            avg(systolic_bp) as systolic_bp,
            avg(body_temp) as body_temp,
            sum(urine) as urine
            from timetable
            group by round(extract('epoch' from time)/1800), hadm_id, icustay_id
            order by hadm_id, time
        );

        alter table timebucket
            add column t_since real,
            add column t_til real;
        update timebucket
            set t_since = extract(epoch from time - c.admittime)/60.0/60.0
            from context c
            where c.hadm_id = timebucket.hadm_id;
        update timebucket
            set t_til = extract(epoch from c.dischtime - time)/60.0/60.0
            from context c
            where c.hadm_id = timebucket.hadm_id;
        """

        cursor.execute(bucket_string)
        con.commit()


        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        yield CreateTimeTable()
    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))

class CreateDayBucket(luigi.Task):
    name = 'CreateDayBucket'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        ######Creating time buckets######
        # if anything happens between 4 and 430 it gets looped to 4 ~ JQ
        bucket_string = """
        drop table if exists daybucket cascade;
        create table daybucket as (
            select timestamp 'epoch' + interval '1 second' * round(extract('epoch' from time)/86400)*86400
            as time, hadm_id, icustay_id,
            avg(pao2_fio2_ratio) as pao2_fio2_ratio,
            avg(potassium) as potassium,
            avg(creatinine) as creatinine,
            avg(hematocrit) as hematocrit,
            avg(serum_bicarb) as serum_bicarb,
            avg(bun) as bun,
            avg(sodium) as sodium,
            avg(chloride) as chloride,
            avg(wbc) as wbc,
            avg(bilirubin) as bilirubin,
            avg(body_temp) as body_temp
            from daily
            group by round(extract('epoch' from time)/86400), hadm_id, icustay_id
            order by hadm_id, time
        );

        alter table daybucket
            add column t_since real,
            add column t_til real;
        update daybucket
            set t_since = extract(epoch from time - c.admittime)/60.0/60.0
            from context c
            where c.hadm_id = daybucket.hadm_id;
        update daybucket
            set t_til = extract(epoch from c.dischtime - time)/60.0/60.0
            from context c
            where c.hadm_id = daybucket.hadm_id;
        """

        cursor.execute(bucket_string)
        con.commit()


        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        yield CreateDaily()
    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))

class RemapContext(luigi.Task):
    name = 'RemapContext'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()
        engine = create_engine('postgresql+psycopg2://{0}:{1}@localhost/{2}'.format(self.user,
                                                                                    self.password,
                                                                                    self.dbname))

        #remapping object oriented columns
        df1 = pd.read_csv(package_location + 'remap_ethnicity.csv')
        df1.columns = [x.lower() for x in df1.columns]
        df2 = pd.read_csv(package_location + 'remap_gender_GenderM.csv')
        df2.columns = [x.lower() for x in df2.columns]
        df3 = pd.read_csv(package_location + 'remap_language.csv')
        df3.columns = [x.lower() for x in df3.columns]
        df4 = pd.read_csv(package_location + 'remap_marital_status.csv')
        df4.columns = [x.lower() for x in df4.columns]
        df5 = pd.read_csv(package_location + 'remap_religion.csv')
        df5.columns = [x.lower() for x in df5.columns]
        
        remap_list = ['ethnicity','gender','language','marital_status','religion']
        
        for r in remap_list:
            cursor.execute('drop table if exists remap_{0}'.format(r))
            con.commit()
        
        df1.to_sql('remap_ethnicity',engine)
        con.commit()
        df2.to_sql('remap_gender',engine)
        con.commit()
        df3.to_sql('remap_language',engine)
        con.commit()
        df4.to_sql('remap_marital_status',engine)
        con.commit()
        df5.to_sql('remap_religion',engine)
        con.commit()
        
        remap_strng = """
        UPDATE context
            SET {0} = r.remap0
            FROM remap_{0} r
            WHERE context.{0} = r.category;
        """
        for r in remap_list:
            cursor.execute(remap_strng.format(r))
            con.commit()
            
        # context = pd.read_sql('select * from context;',con)
        # dummies_list=['admission_type','insurance','language','religion','marital_status','ethnicity','gender']
        # context = pd.get_dummies(context, columns=dummies_list)      

        # cursor.execute('drop table if exists context;')
        # con.commit()
        # context.to_sql('context',engine)
        # con.commit()


        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))
    def requires(self):
        yield CreateContextWithTimeFix()
        yield AddServices()
        yield FilterDiagnoses()
    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))



class TrainTestSplitter(luigi.Task):
    name = 'TrainTestSplitter'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        X_train = pd.read_csv('train_hadm_ids.csv')['hadm_id'].tolist()
        train_adm = 'ANY( VALUES (' + ') ,('.join([str(u) for u in X_train]) + '))'
        X_test = pd.read_csv('test_hadm_ids.csv')['hadm_id'].tolist()
        test_adm = 'ANY( VALUES (' + ') ,('.join([str(u) for u in X_test]) + '))'

        create_splits = """
        drop table if exists train_context;
        drop table if exists train_timebucket;
        drop table if exists train_daybucket;
        drop table if exists train_timetable;
        drop table if exists test_context;
        drop table if exists test_timebucket;
        drop table if exists test_daybucket;
        drop table if exists test_timetable;



        create table train_context as (
        select * from context where hadm_id = {0}
        );
        create table train_timebucket as (
        select * from timebucket where hadm_id = {0}
        );
        create table train_daybucket as (
        select * from daybucket where hadm_id = {0}
        );
        create table train_timetable as (
        select * from timetable where hadm_id = {0}
        );



        create table test_context as (
        select * from context where hadm_id = {1}
        );
        create table test_timebucket as (
        select * from timebucket where hadm_id = {1}
        );
        create table test_daybucket as (
        select * from daybucket where hadm_id = {1}
        );
        create table test_timetable as (
        select * from timetable where hadm_id = {1}
        );


        """.format(train_adm, test_adm)

        cursor.execute(create_splits)
        con.commit()


        with self.output().open('w') as f:
            f.write('completed on {}'.format(datetime.datetime.now()))

    def requires(self):
        yield RemapContext()
        yield CreateTimeBucket()
        yield CreateDayBucket()
    def output(self):
        return luigi.LocalTarget(package_location + 'output/{0}.txt'.format(self.name))



class EndTimer(luigi.Task):
    def requires(self):
        return TrainTestSplitter()

    def run(self):
        with self.output().open('w') as f:
            f.write('theoverall process took {} minutes'.format((time()-starttime)/60.0))

    def output(self):
        return luigi.LocalTarget(package_location + 'output/zzz.txt')

class GetSplit(luigi.Task):
    name = 'TrainTestSplitter'
    dbname = luigi.Parameter(default=targetdb)
    user = luigi.Parameter(default=sqluser)
    password = luigi.Parameter(default=pw)
    host = luigi.Parameter(default='/var/run/postgresql/')

    def run(self):
        con=psycopg2.connect(user=self.user,
                             password=self.password,
                             host=self.host,
                             dbname=self.dbname)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()

        df = pd.read_sql('select subject_id, hadm_id, died from context where hadm_id is not null', con)
        X = df[['subject_id','hadm_id']].values
        y = df[['died']].values
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, stratify = y)

        X_train_df = pd.DataFrame(X_train)
        X_train_df = X_train_df.rename(columns = {0:'subject_id',1:'hadm_id'})
        X_train_df.to_csv('train_hadm_ids.csv')

        X_test_df = pd.DataFrame(X_test)
        X_test_df = X_test_df.rename(columns = {0:'subject_id',1:'hadm_id'})
        X_test_df.to_csv('test_hadm_ids.csv')



class Run_001(luigi.WrapperTask):
    def requires(self):
        yield CreateDb()
        yield GetAdm()
        yield CreateContextWithTimeFix()
        yield AddServices()
        #yield ItemIdList()
        yield ItemIdList2()
        yield FilterDiagnoses()
        #yield FilterInputItemId()
        #yield UploadValidUOM()
        #yield FilterInputCV()
        #yield FilterInputMV()
        yield FilterOutputItemId()
        yield FilterChartItemId()
        yield FilterLabItemId()
        # yield FilterMicrobioItemId()
        # yield FilterPrescriptItemId()
        yield FixChartItemId()
        yield FixOutputItemId()
        yield FixLabItemId()
        yield CreateTimeTable()
        yield CreateDaily()
        yield CreateTimeBucket()
        yield CreateDayBucket()
        yield RemapContext()
        yield TrainTestSplitter()
        yield EndTimer()



if __name__ == '__main__':
    luigi.run()
