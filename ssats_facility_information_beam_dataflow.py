#dataflow

import os, datetime, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


class FormatDataFn(beam.DoFn):
    def process(self,element):
        data = element
        caseid = data.get('CASEID')
        state = data.get('STATE')
        ctypehi2 = data.get('CTYPEHI2')
        ctype1 = data.get('CTYPE1')
        ownershp = data.get('OWNERSHP')
        
        
        #reformat data to represent what it means based on information from the codebook
        
        #ctype1 is whether the facility offers any outpatient substance abuse service 
        #0 means no and 1 means yes
        
        if ctype1 == 0:
            ctype1 = 'No'
        if ctype1 == 1:
            ctype1 = 'Yes'
            

    
        
        return [{
            'CASEID': data['CASEID'],
            'STATE': state,
            'CTYPEHI2': data['CTYPEHI2'],
            'CTYPE1': ctype1,
            'OWNERSHP' : data['OWNERSHP'],
            }]
 


def run():
     PROJECT_ID = 'first-planet-266901' # change to your project id

     BUCKET = 'gs://beam_team_foodies'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

     # run pipeline on Dataflow 
     options = {
         'runner': 'DataflowRunner',
         'job_name': 'transform-ssats-facility-information-df',
         'project': PROJECT_ID,
         'temp_location': BUCKET + '/temp',
         'staging_location': BUCKET + '/staging',
         'machine_type': 'n1-standard-4', # https://cloud.google.com/compute/docs/machine-types
         'num_workers': 1
     }



     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DataflowRunner', options=opts)
        
      

    
     sql = 'SELECT CASEID,STATE, CTYPEHI2, CTYPE1,OWNERSHP FROM ssats_modeled.facility_information'
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
     table_id = 'ssats_facility_information_Beam_DF'
     schema_id = 'CASEID:STRING,STATE:STRING,CTYPEHI2:STRING,CTYPE1:STRING,OWNERSHP:INTEGER'
        

    
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


