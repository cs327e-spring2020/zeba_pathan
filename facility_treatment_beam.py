import logging
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

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)
    
     sql = 'SELECT CASEID, TREATMT, TREATGRPTHRPY, TREATTRAUMATHRPY, ALZHDEMENTIA, SRVC31,SPECGRPEATING,TRAUMATICBRAIN FROM samhda_modeled.facility_treatment limit 20'
        
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
     
     #write to input file
     query_results | 'Write log input' >> WriteToText('input.txt')
        
     # apply ParDo to format the data 
     formatted_data_pcoll = query_results | 'Format Data' >> beam.ParDo(FormatDataFn())
        
     # write PCollection to log file
     formatted_data_pcoll | 'Write log 1' >> WriteToText('output.txt')
        
    
    
     dataset_id = 'samhda_modeled'
     table_id = 'facility_treatment_Beam'
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


