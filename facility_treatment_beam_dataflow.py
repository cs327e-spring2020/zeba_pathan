import os, datetime, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText




class FormatDataFn(beam.DoFn):
    def process(self,element):
        data = element
        caseid = data.get('CASEID')
        treatmt = data.get('TREATMT')
        treatgrpthrpy = data.get('TREATGRPTHRPY')
        treattraumathrpy = data.get('TREATTRAUMATHRPY')
        alzhdementia = data.get('ALZHDEMENTIA')
        srvc31 = data.get('SRVC31')
        specgrpeating = data.get('SPECGRPEATING')
        traumaticbrain = data.get('TRAUMATICBRAIN')
        
        
        #reformat data to represent what it means based on information from the codebook
        
        #treatgrptherapy is whether the facility offers group therapy
        #0 means no, 1 means yes, and -1 means missing
        
        if treatgrpthrpy == 0:
            treatgrpthrpy = 'No'
        if treatgrpthrpy == 1:
            treatgrpthrpy = 'Yes'
        if treatgrpthrpy == -1:
            treatgrpthrpy = 'Missing'
        if treatgrpthrpy == -5:
            treatgrpthrpy= 'Refused'
                    
                       
    
    
        
        return [{
            'CASEID': data['CASEID'],
            'TREATMT': data['TREATMT'],
            'TREATGRPTHRPY': treatgrpthrpy,
            'TREATTRAUMATHRPY': data['TREATTRAUMATHRPY'],
            'ALZHDEMENTIA': data['ALZHDEMENTIA'],
            'SRVC31': data['SRVC31'],
            'SPECGRPEATING' : data['SPECGRPEATING'],
            'TRAUMATICBRAIN' : data['TRAUMATICBRAIN'],
            }]

    
    
    
def run():
     PROJECT_ID = 'first-planet-266901' # change to your project id

     BUCKET = 'gs://beam_team_foodies'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

     # run pipeline on Dataflow 
     options = {
         'runner': 'DataflowRunner',
         'job_name': 'transform-facility-treatment',
         'project': PROJECT_ID,
         'temp_location': BUCKET + '/temp',
         'staging_location': BUCKET + '/staging',
         'machine_type': 'n1-standard-4', # https://cloud.google.com/compute/docs/machine-types
         'num_workers': 1
     }


     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DataflowRunner', options=opts)
    
     sql = 'SELECT CASEID, TREATMT, TREATGRPTHRPY, TREATTRAUMATHRPY, ALZHDEMENTIA, SRVC31,SPECGRPEATING,TRAUMATICBRAIN FROM samhda_modeled.facility_treatment'
        
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
     
     #write to input file
     query_results | 'Write log input' >> WriteToText('input_df.txt')
        
     # apply ParDo to format the data 
     formatted_data_pcoll = query_results | 'Format Data' >> beam.ParDo(FormatDataFn())
        
     # write PCollection to log file
     formatted_data_pcoll | 'Write log 1' >> WriteToText('output_df.txt')
        
    
    
     dataset_id = 'samhda_modeled'
     table_id = 'facility_treatment_Beam_DF'
     schema_id = 'CASEID:STRING,TREATMT:STRING,TREATGRPTHRPY:STRING,TREATTRAUMATHRPY:INTEGER,ALZHDEMENTIA:INTEGER,SRVC31:INTEGER,SPECGRPEATING:INTEGER,TRAUMATICBRAIN:INTEGER'
        

    
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


