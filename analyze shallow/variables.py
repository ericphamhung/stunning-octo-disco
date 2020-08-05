import pandas as pd
from time import time



sqluser = 'postgres'
schema_name = 'mimiciii'
password = 'postgres'
targetdb = 'test'


#CreateContextPlus()
#selectitem = "itemid2"
# pivotstring = '''select icustay_id, avg (value) average_{0}, min(value) minimum_{0}, max(value) maximum_{0}
#     , count(*) cnt_{0}
#     from timetable
#     where {1} = '{0}'
#     group by icustay_id 
#     order by icustay_id;
# 
#     '''

#TODO ADD COALESCE
ce_pivot = '''select icustay_id, avg(valuenum) avrg_{0}_{1}, min(valuenum) mini_{0}_{1}
    , max(valuenum) maxi_{0}_{1}
    , count(valuenum)/los_total cnt_{0}_{1}
    from chartevents
    where cid1 ={0} AND cid2 = {1}
    group by icustay_id, cid1, cid2, los_total
    order by icustay_id, cid1, cid2;
 
    '''

mv_out_pivot = '''select icustay_id, sum({2})/los_total tot_{0}_{1}_{4}
    from {3}
    where cid1 = {0} AND cid2 = {1}
    group by icustay_id, cid1, cid2, los_total
    order by icustay_id, cid1, cid2 ;
 
    ''' 

selectitem = 'nitemid'




#StandardizeData()

contextselector_old = '''
select * 
    ,extract(epoch from admittime - dob )/3600 as age_at_admission
    ,extract(epoch from intime - admittime )/3600 as adm_to_icu
    ,extract(epoch from outtime - intime )/3600 as icu_los
    ,extract(epoch from dischtime - admittime )/3600 as hosp_los
    ,extract(epoch from dod - dob )/3600 as age_at_death
    
    from context
    
'''

context_for_context = '''
select distinct({}) from context;
'''
cxtlst = ['dbsource', 'first_careunit', 'last_careunit', 'admission_type',
       'admission_location', 'discharge_location', 'religion', 'marital_status',
       'ethnicity', 'gender']

contextselector_temporary = '''
select c.icustay_id, c.died as target_died, 
     c.first_careunit,c.last_careunit, c.admission_type, 
     c.admission_location, c.discharge_location,
     c.marital_status, c.ethnicity, c.gender,
     c.insurance, c.language, c.religion,
     c.adm_rank, c.icu_rank
     ,extract(epoch from c.admittime - c.dob )/3600 as age_at_admission
    ,extract(epoch from c.intime - c.admittime )/3600 as adm_to_icu
    ,extract(epoch from c.outtime - c.intime )/3600 as target_icu_los
    ,extract(epoch from c.dischtime - c.admittime )/3600 as target_hosp_los
    ,extract(epoch from c.dod - c.dob )/3600 as age_at_death
    , s.curr_service 
    from context c inner join services s 
    on c.hadm_id = s.hadm_id;  
'''

pivotstring_ce = '''select icustay_id, avg (value) average_{0}, min(value) minimum_{0}, max(value) maximum_{0}
    , count(*) cnt_{0}
    from timetable
    where (nitemid/100 = {0}) and (nitemid/10000000 = 3)
    group by icustay_id 
    order by icustay_id;
    '''
    
pivotstring_oe = '''select icustay_id, sum(value) sum_{0}, min(value) minimum_{0}, max(value) maximum_{0}
    , count(*) cnt_{0}
    from timetable
    where (nitemid/100 = {0}) and (nitemid/10000000 = 3)
    group by icustay_id 
    order by icustay_id;
    '''    

pivotstring_ie = '''
select icustay_id, sum(amount) sum_{0}, sum(elapsed) time_{0},
        count(*) cnt_{0}
        from inputevents_mv
    where (nitemid/100 = {0})
    group by icustay_id
    order by icustay_id;
'''


pivotstring_ie_badder = '''
select icustay_id, nitemid, time, value 
from timetable where (nitemid/10000000 = 2)
    order by icustay_id, nitemid, time;
'''


pivotstring_ie_bad = '''
create table temp_start as 
select icustay_id, nitemid, value, time, flag from timetable 
where (log("nitemid")>7.3) and (log("nitemid")<7.477) and flag = 0 order by icustay_id, nitemid, time;
select count(*) from temp_start;
create table temp_finish as 
select icustay_id, nitemid, value, time, flag from timetable 
where (log("nitemid")>7.3) and (log("nitemid")<7.477) and flag=1 order by icustay_id, nitemid, time;
select count(*) from temp_finish;
create table temp as
select s.icustay_id, s.nitemid, f.time - s.time as elapsed, f.value
from temp_start s inner join temp_finish f
on s.icustay_id = f.icustay_id and s.nitemid = f.nitemid;
select count(*) from temp;
select icustay_id, sum(value) sum_{0}, min(value) minimum_{0}, max(value) maximum_{0}
    , count(*) cnt_{0}
    from timetable
    where (nitemid/100 = {0}) and "table" = 'outputevents'
    group by icustay_id 
    order by icustay_id;
    '''    

# converts categorical features to numerical
# def stringtodict(df,col):
#     a = df[col].unique().tolist()
#     b={}
#     n=0
#     for i in a:
#         b[i] = n
#         n += 1
#     return b


#timesummary = '''
#        select icustay_id, avg ({1}) average_{0}, min({1}) minimum_{0}, max({1}) maximum_{0}
#        , count(*) cnt_{0}
#        from {2}
#        where class2 = '{0}'
#        group by icustay_id, nitemid 
#        order by icustay_id, nitemid;

#        '''

#timetables = {'outputevents':'value'
#              ,'chartevents':'valuenum'
#              ,'inputevents_mv':'amount'
#             }

