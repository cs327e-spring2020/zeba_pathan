import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 5, 2)
}

staging_dataset = 'ssats_workflow_staging'
modeled_dataset = 'ssats_workflow_modeled'

bq_query_start = 'bq query --use_legacy_sql=false '

generate_caseid_2018 = 'create or replace table ' + staging_dataset + '''.ssats_2018_u as
                    SELECT generate_uuid() as CASEID, *
                    FROM ssats_staging. ssats_2018
                    WHERE CASEI_3 IS NOT NULL'''

generate_caseid_2017 = 'create or replace table ' + staging_dataset + '''.ssats_2017_u as
                    SELECT generate_uuid() as CASEID, *
                    FROM ssats_staging. ssats_2017
                    WHERE CASEI_3 IS NOT NULL'''

generate_caseid_2016 = 'create or replace table ' + staging_dataset + '''.ssats_2016_u as
                    SELECT generate_uuid() as CASEID, *
                    FROM ssats_staging. ssats_2016
                    WHERE CASEI_3 IS NOT NULL'''

generate_caseid_2015 = 'create or replace table ' + staging_dataset + '''.ssats_2015_u as
                    SELECT generate_uuid() as CASEID, *
                    FROM ssats_staging. ssats_2015
                    WHERE CASEI_3 IS NOT NULL'''

generate_caseid_2014 = 'create or replace table ' + staging_dataset + '''.ssats_2014_u as
                    SELECT generate_uuid() as CASEID, *
                    FROM ssats_staging. ssats_2014
                    WHERE CASEI_3 IS NOT NULL'''


##naming is a little different than it was in ssats_modeled.
#ssats_workflow_modeled.ssats_facility_information is the same as ssats_modeled.facility_information
#ssats_workflow_modeled.ssats_facility_services is the same as ssats_modeled.facility_services
#ssats_workflow_modeled.ssats_facility_treatment is the same as ssats_modeled.facility_treatment
#caseid was also generated using (generate_uuid()) so it wont be the same in ssats_modeled and ssats_workflow_modeled


create_information_sql = 'create or replace table ' + modeled_dataset + '''.ssats_facility_information as
                      SELECT DISTINCT generate_uuid() as CASEID, STATE, CAST(CASE WHEN CTYPEHI2 = 1 THEN \"Yes\" WHEN CTYPEHI2 = 0 THEN \"No\" ELSE Null END as String) as CTYPEHI2, CTYPE1, OWNE_2SHP AS OWNERSHP
                      from ''' + staging_dataset + '''.ssats_2018_u
                      WHERE CASEI_3 IS NOT NULL
                      union distinct
                      SELECT DISTINCT generate_uuid() as CASEID, STATE, CAST(CASE WHEN CTYPEHI2 = 1 THEN \"Yes\" WHEN CTYPEHI2 = 0 THEN \"No\" ELSE Null END as String) as CTYPEHI2, CTYPE1, OW_5E_2SHP AS OWNERSHP
                      from ''' + staging_dataset + '''.ssats_2017_u
                      WHERE CASEI_3 IS NOT NULL
                      union distinct 
                      SELECT DISTINCT generate_uuid() as CASEID, STATE, CAST(CASE WHEN CTYPEHI2 = 1 THEN \"Yes\" WHEN CTYPEHI2 = 0 THEN \"No\" ELSE Null END as String) as CTYPEHI2, CTYPE1, OWNE_2SHP AS OWNERSHP
                      from ''' + staging_dataset + '''.ssats_2016_u
                      WHERE CASEI_3 IS NOT NULL
                      union distinct 
                      SELECT DISTINCT generate_uuid() as CASEID, STATE, CAST(CASE WHEN CTYPEHI2 = 1 THEN \"Yes\" WHEN CTYPEHI2 = 0 THEN \"No\" ELSE Null END as String) as CTYPEHI2, CTYPE1, OWNE_2SHP AS OWNERSHP
                      from ''' + staging_dataset + '''.ssats_2015_u
                      WHERE CASEI_3 IS NOT NULL
                      union distinct 
                      SELECT DISTINCT generate_uuid() as CASEID, STATE, CAST(CASE WHEN CTYPEHI2 = 1 THEN \"Yes\" WHEN CTYPEHI2 = 0 THEN \"No\" ELSE Null END as String) as CTYPEHI2, CTYPE1, OWNE_2SHP AS OWNERSHP
                      from ''' + staging_dataset + '''.ssats_2014_u
                      WHERE CASEI_3 IS NOT NULL'''
                        


create_services_sql = 'create or replace table ' + modeled_dataset + '''.ssats_facility_services as
                    SELECT DISTINCT CASEID, CAST(CASE WHEN SIGNLANG = 1 THEN \"Yes\" WHEN SIGNLANG = 0 THEN \"No\" ELSE Null END as String) as SIGNLANG, CTYPE_1L AS CTYPEML, LANG16, S_2VC108 AS SRVC108,S_2VC113 AS SRVC113,
                    FROM ''' + staging_dataset + '''.ssats_2018_u
                    WHERE CASEID IS NOT NULL
                    UNION DISTINCT
 
                    SELECT DISTINCT CASEID, CAST(CASE WHEN SIG_5LA_5G = 1 THEN \"Yes\" WHEN SIG_5LA_5G = 0 THEN \"No\" ELSE Null END as String) as SIGNLANG, CTYPE_1L AS CTYPEML, LA_5G16 AS LANG16, S_2VC108 AS SRVC108,S_2VC113 AS SRVC113,
                    from ''' + staging_dataset + '''.ssats_2017_u
                    WHERE CASEID IS NOT NULL
                    UNION DISTINCT
 
                    SELECT DISTINCT CASEID, CAST(CASE WHEN SIGNLANG = 1 THEN \"Yes\" WHEN SIGNLANG = 0 THEN \"No\" ELSE Null END as String) as SIGNLANG, CTYPE_1L AS CTYPEML, LANG16, S_2VC108 AS SRVC108,S_2VC113 AS SRVC113,
                    from ''' + staging_dataset + '''.ssats_2016_u
                    WHERE CASEID IS NOT NULL
                    UNION DISTINCT 
                    SELECT DISTINCT CASEID, CAST(CASE WHEN SIGNLANG = 1 THEN \"Yes\" WHEN SIGNLANG = 0 THEN \"No\" ELSE Null END as String) as SIGNLANG, CTYPE_1L AS CTYPEML, LANG16, S_2VC108 AS SRVC108,S_2VC113 AS SRVC113,
                    from ''' + staging_dataset + '''.ssats_2015_u
                    WHERE CASEID IS NOT NULL
                    UNION DISTINCT
 
                    SELECT DISTINCT CASEID, CAST(CASE WHEN SIGNLANG = 1 THEN \"Yes\" WHEN SIGNLANG = 0 THEN \"No\" ELSE Null END as String) as SIGNLANG, CTYPE_1L AS CTYPEML, LANG16, S_2VC108 AS SRVC108,S_2VC113 AS SRVC113,
                    from ''' + staging_dataset + '''.ssats_2014_u
                    WHERE CASEID IS NOT NULL'''
                    

create_treatment_sql = 'create or replace table ' + modeled_dataset + '''.ssats_facility_treatment as
                    SELECT DISTINCT CASEID, OTP, CAST(CASE WHEN T_2EAT_1T = 1 THEN \"Yes\" WHEN T_2EAT_1T = 0 THEN \"No\" ELSE Null END as String) as TREATMT, S_2VC71 AS SRVC71, CTYPEHI1, CTYPE_2C1 AS CTYPERC1, S_2VC85 AS SRVC85,
                    from ''' + staging_dataset + '''.ssats_2018_u
                    WHERE CASEID IS NOT NULL
                    UNION DISTINCT
                    
                    SELECT DISTINCT CASEID, OTP, CAST(CASE WHEN T_2EAT_1T = 1 THEN \"Yes\" WHEN T_2EAT_1T = 0 THEN \"No\" ELSE Null END as String) as TREATMT, S_2VC71 AS SRVC71, CTYPEHI1, CTYPE_2C1 AS CTYPERC1, S_2VC85 AS SRVC85,
                    from ''' + staging_dataset + '''.ssats_2017_u
                    WHERE CASEID IS NOT NULL
                    UNION DISTINCT
                    
                    SELECT DISTINCT CASEID, OTP, CAST(CASE WHEN T_2EAT_1T = 1 THEN \"Yes\" WHEN T_2EAT_1T = 0 THEN \"No\" ELSE Null END as String) as TREATMT, S_2VC71 AS SRVC71, CTYPEHI1, CTYPE_2C1 AS CTYPERC1, S_2VC85 AS SRVC85,
                    from ''' + staging_dataset + '''.ssats_2016_u
                    WHERE CASEID IS NOT NULL
                    UNION DISTINCT
                    
                    SELECT DISTINCT CASEID, OTP, CAST(CASE WHEN T_2EAT_1T = 1 THEN \"Yes\" WHEN T_2EAT_1T = 0 THEN \"No\" ELSE Null END as String) as TREATMT, S_2VC71 AS SRVC71, CTYPEHI1, CTYPE_2C1 AS CTYPERC1, S_2VC85 AS SRVC85,
                    from ''' + staging_dataset + '''.ssats_2015_u
                    WHERE CASEID IS NOT NULL
                    UNION DISTINCT
                    
                    SELECT DISTINCT CASEID, OTP, CAST(CASE WHEN T_2EAT_1T = 1 THEN \"Yes\" WHEN T_2EAT_1T = 0 THEN \"No\" ELSE Null END as String) as TREATMT, S_2VC71 AS SRVC71, CTYPEHI1, CTYPE_2C1 AS CTYPERC1, S_2VC85 AS SRVC85,
                    from ''' + staging_dataset + '''.ssats_2014_u
                    WHERE CASEID IS NOT NULL'''
                    

with models.DAG(
        'ssats_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_staging = BashOperator(
            task_id='create_staging_dataset',
            bash_command='bq --location=US mk --dataset ' + staging_dataset)
    
    create_modeled = BashOperator(
            task_id='create_modeled_dataset',
            bash_command='bq --location=US mk --dataset ' + modeled_dataset)
    
    
    
    load_ssats_2018 = BashOperator(
            task_id='load_ssats_2018',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.ssats_2018 \
                         "gs://databases-project/SubstanceAbuseDatasets/SSATS_2018.csv"',
            trigger_rule='all_done')
    
    load_ssats_2017 = BashOperator(
            task_id='load_ssats_2017',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.ssats_2017 \
                         "gs://databases-project/SubstanceAbuseDatasets/SSATS_2017.csv"',
            trigger_rule='all_done')
    
    load_ssats_2016 = BashOperator(
            task_id='load_ssats_2016',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.ssats_2016 \
                         "gs://databases-project/SubstanceAbuseDatasets/SSATS_2016.csv"', 
            trigger_rule='all_done')
    
    load_ssats_2015 = BashOperator(
            task_id='load_ssats_2015',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.ssats_2015 \
                         "gs://databases-project/SubstanceAbuseDatasets/SSATS_2015.csv"', 
            trigger_rule='all_done')   
    
    load_ssats_2014 = BashOperator(
            task_id='load_ssats_2014',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.ssats_2014 \
                         "gs://databases-project/SubstanceAbuseDatasets/SSATS_2014.csv"', 
            trigger_rule='all_done')   
    
    
    branch = DummyOperator(
            task_id='branch',
            trigger_rule='all_done')
    
    branch2 = DummyOperator(
            task_id='branch',
            trigger_rule='all_done')
    
    join = DummyOperator(
            task_id='join',
            trigger_rule='all_done')


    uuid_2018 = BashOperator(
            task_id='uuid_2018',
            bash_command=bq_query_start + "'" + generate_caseid_2018 + "'", 
            trigger_rule='all_done')
    
    uuid_2017 = BashOperator(
            task_id='uuid_2017',
            bash_command=bq_query_start + "'" + generate_caseid_2017 + "'", 
            trigger_rule='all_done')
    
    uuid_2016 = BashOperator(
            task_id='uuid_2016',
            bash_command=bq_query_start + "'" + generate_caseid_2016 + "'", 
            trigger_rule='all_done')
    
    uuid_2015 = BashOperator(
            task_id='uuid_2015',
            bash_command=bq_query_start + "'" + generate_caseid_2015 + "'", 
            trigger_rule='all_done')
    
    uuid_2014 = BashOperator(
            task_id='uuid_2014',
            bash_command=bq_query_start + "'" + generate_caseid_2014 + "'", 
            trigger_rule='all_done')




    
    create_information = BashOperator(
            task_id='create_information',
            bash_command=bq_query_start + "'" + create_information_sql + "'", 
            trigger_rule='all_done')
    
    create_services = BashOperator(
            task_id='create_services',
            bash_command=bq_query_start + "'" + create_services_sql + "'", 
            trigger_rule='all_done')
    
    create_treatment = BashOperator(
            task_id='create_treatment',
            bash_command=bq_query_start + "'" + create_treatment_sql + "'", 
            trigger_rule='all_done')
        
    information = BashOperator(
            task_id='information',
            bash_command='python /home/jupyter/airflow/dags/ssats_facility_information_beam_dataflow2.py')
    
    services = BashOperator(
            task_id='services',
            bash_command='python /home/jupyter/airflow/dags/ssats_facility_services_beam_dataflow2.py')
    
    treatment = BashOperator(
            task_id='treatment',
            bash_command='python /home/jupyter/airflow/dags/ssats_facility_treatment_beam_dataflow2.py')
    
    
    create_staging >> create_modeled >> branch
    load_ssats_2018 >> uuid_2018 >> join
    load_ssats_2017 >> uuid_2017 >> join
    load_ssats_2016 >> uuid_2016 >>join
    load_ssats_2015 >> uuid_2015 >> join
    load_ssats_2014 >> uuid_2014 >> join >> branch2
    branch >> create_information >> information
    branch >> create_services >> services
    branch >> create_treatment >> treatment
     
    
    

    

   # branch >> load_class >> create_class >> create_teacher >> create_teaches