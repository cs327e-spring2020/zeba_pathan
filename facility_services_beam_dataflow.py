import os, datetime, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText




class FormatDataFn(beam.DoFn):
    def process(self,element):
        data = element
        caseid = data.get('CASEID')
        mhintcasemgmt = data.get('MHINTCASEMGMT')
        mhchronic = data.get('MHCHRONIC')
        illnessmgmt = data.get('ILLNESSMGMT')
        primarycare = data.get('PRIMARYCARE')
        fampsyched = data.get('FAMPSYCHED')
        supphousing = data.get('SUPPHOUSING')
        suppemploy = data.get('SUPPEMPLOY')
        mhlegal = data.get('MHLEGAL')
        mhemgcy = data.get('MHEMGCY')
        mhsuicide = data.get('MHSUICIDE')

        
        
        #reformat data to represent what it means based on information from the codebook
        
        #mhlegal is whether the facility offers legal advocacy 
        #0 means no, 1 means yes, -1 means missing
        
        if mhlegal == 0:
            mhlegal = 'No'
        if mhlegal == 1:
            mhlegal = 'Yes'
        if mhlegal == -1:
            mhlegal = 'Missing'
                    
                       
    
    
        
        return [{
            'CASEID': data['CASEID'],
            'MHINTCASEMGMT': data['MHINTCASEMGMT'],
            'MHCHRONIC': data['MHCHRONIC'],
            'ILLNESSMGMT': data['ILLNESSMGMT'],
            'PRIMARYCARE': data['PRIMARYCARE'],
            'FAMPSYCHED': data['FAMPSYCHED'],
            'SUPPHOUSING' : data['SUPPHOUSING'],
            'SUPPEMPLOY' : data['SUPPEMPLOY'],
            'MHLEGAL': mhlegal,
            'MHEMGCY': data['MHEMGCY'],
            'MHSUICIDE': data['MHSUICIDE']
            }]
    

 


    
def run():
     PROJECT_ID = 'first-planet-266901' # change to your project id

     BUCKET = 'gs://beam_team_foodies'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

     # run pipeline on Dataflow 
     options = {
         'runner': 'DataflowRunner',
         'job_name': 'transform-facility-services',
         'project': PROJECT_ID,
         'temp_location': BUCKET + '/temp',
         'staging_location': BUCKET + '/staging',
         'machine_type': 'n1-standard-4', # https://cloud.google.com/compute/docs/machine-types
         'num_workers': 1
     }


     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DataflowRunner', options=opts)
    
     sql = 'SELECT CASEID, MHINTCASEMGMT, MHCHRONIC, ILLNESSMGMT, PRIMARYCARE, FAMPSYCHED,SUPPHOUSING,SUPPEMPLOY, MHLEGAL, MHEMGCY, MHSUICIDE FROM samhda_modeled.facility_services'
        
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
     
     #write to input file
     query_results | 'Write log input' >> WriteToText('input.txt')
        
     # apply ParDo to format the data 
     formatted_data_pcoll = query_results | 'Format Data' >> beam.ParDo(FormatDataFn())
        
     # write PCollection to log file
     formatted_data_pcoll | 'Write log 1' >> WriteToText('output.txt')
        
    
    
     dataset_id = 'samhda_modeled'
     table_id = 'facility_services_Beam_DF'
     schema_id = 'CASEID:STRING,MHINTCASEMGMT:INTEGER,MHCHRONIC:STRING,ILLNESSMGMT:INTEGER,PRIMARYCARE:INTEGER,FAMPSYCHED:INTEGER,SUPPHOUSING:INTEGER,SUPPEMPLOY:INTEGER, MHLEGAL:STRING, MHEMGCY:INTEGER, MHSUICIDE:INTEGER' 
        

    
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


