#dataflow
import os, datetime, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

##treatment dataflow 

class FormatDataFn(beam.DoFn):
    def process(self,element):
        data = element
        caseid = data.get('CASEID')
        treatmt = data.get('TREATMT')
        srvc71 = data.get('SRVC71')
        otp = data.get('OTP')
        ctypehi1 = data.get('CTYPEHI1')
        ctyperc1 = data.get('CTYPERC1')
        srvc85 = data.get('SRVC85')

        
        #reformat data to represent what it means based on information from the codebook
        
        #srvc85 is whether the  Facility offers methadone pharmacotherapy services
        #0 means no, 1 means yes, and -1 means missing
        
        
        if srvc85 == 0:
            srvc85 = 'No'
        if srvc85 == 1:
            srvc85 = 'Yes'
        if srvc85 == -1:
            srvc85 = 'Missing'
        if srvc85 == -2:
            srvc85 = 'Refused'
        if srvc85 == -3:
            srvc85 = 'Do not know'
                    
                       
    
    
        
        return [{
            'CASEID': data['CASEID'],
            'TREATMT': data['TREATMT'],
            'SRVC71': data['SRVC71'],
            'OTP': data['OTP'],
            'CTYPEHI1': data['CTYPEHI1'],
            'CTYPERC1' : data['CTYPERC1'],
            'SRVC85' : srvc85,
            }]


def run():
     PROJECT_ID = 'first-planet-266901' # change to your project id

     BUCKET = 'gs://beam_team_foodies'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

     # run pipeline on Dataflow 
     options = {
         'runner': 'DataflowRunner',
         'job_name': 'transform-ssats-facility-treatment',
         'project': PROJECT_ID,
         'temp_location': BUCKET + '/temp',
         'staging_location': BUCKET + '/staging',
         'machine_type': 'n1-standard-4', # https://cloud.google.com/compute/docs/machine-types
         'num_workers': 1
     }


     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DataflowRunner', options=opts)
        
      

    
     sql = 'SELECT CASEID,TREATMT, SRVC71, OTP, CTYPEHI1,CTYPERC1,SRVC85 FROM ssats_modeled.facility_treatment'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
     
     #write to input file
     query_results | 'Write log input' >> WriteToText('input.txt')
        
     # apply ParDo to format the data 
     formatted_data_pcoll = query_results | 'Format Data' >> beam.ParDo(FormatDataFn())
        
     # write PCollection to log file
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'
     
     formatted_data_pcoll | 'Write log 1' >> WriteToText(DIR_PATH + 'output.txt')
        
    
    
     dataset_id = 'ssats_modeled'
     table_id = 'ssats_facility_treatment_Beam_DF'
     schema_id = 'CASEID:STRING,TREATMT:STRING,SRVC71:INTEGER,OTP:INTEGER,CTYPEHI1:INTEGER,CTYPERC1:INTEGER,SRVC85:STRING'
        

    
     # write PCollection to new BQ table
     formatted_data_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                  batch_size=int(100))
        
        
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()


