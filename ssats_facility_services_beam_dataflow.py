#dataflow
import os, datetime, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

##services dataflow 

class FormatDataFn(beam.DoFn):
    def process(self,element):
        data = element
        caseid = data.get('CASEID')
        signlang = data.get('SIGNLANG')
        ctypeml = data.get('CTYPEML')
        lang16 = data.get('LANG16')
        srvc108 = data.get('SRVC108')
        srvc113 = data.get('SRVC113')
   
        
        
        #reformat data to represent what it means based on information from the codebook
        
        #srvc113 is whether the facility has a program tailored for veterans
        #0 means no, 1 means yes, -1 means missing
        
        if srvc113 == 0:
            srvc113 = 'No'
        if srvc113 == 1:
            srvc113 = 'Yes'
        if srvc113 == -1:
            srvc113 = 'Missing'
        if srvc113 == -2:
            srvc113 = 'Refused'
        if srvc113 == -3:
            srvc113 = 'Do not know'

        
                    
                       
    
    
        
        return [{
            'CASEID': data['CASEID'],
            'SIGNLANG': data['SIGNLANG'],
            'CTYPEML': data['CTYPEML'],
            'LANG16': data['LANG16'],
            'SRVC108': data['SRVC108'],
            'SRVC113' : srvc113,
            }]
    
    
    

 



def run():
     PROJECT_ID = 'first-planet-266901' # change to your project id

     BUCKET = 'gs://beam_team_foodies'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

     # run pipeline on Dataflow 
     options = {
         'runner': 'DataflowRunner',
         'job_name': 'transform-ssats-facility-services',
         'project': PROJECT_ID,
         'temp_location': BUCKET + '/temp',
         'staging_location': BUCKET + '/staging',
         'machine_type': 'n1-standard-4', # https://cloud.google.com/compute/docs/machine-types
         'num_workers': 1
     }


     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DataflowRunner', options=opts)
        
      

    
     sql = 'SELECT CASEID, SIGNLANG, CTYPEML, LANG16, SRVC108,SRVC113 FROM ssats_modeled.facility_services'
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
     table_id = 'ssats_facility_services_Beam_DF'
     schema_id = schema_id = 'CASEID:STRING,SIGNLANG:STRING,CTYPEML:INTEGER,LANG16:INTEGER,SRVC108:INTEGER,SRVC113:STRING' 
        

    
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


