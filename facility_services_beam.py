import logging
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

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)
    
     sql = 'SELECT CASEID, MHINTCASEMGMT, MHCHRONIC, ILLNESSMGMT, PRIMARYCARE, FAMPSYCHED,SUPPHOUSING,SUPPEMPLOY, MHLEGAL, MHEMGCY, MHSUICIDE FROM samhda_modeled.facility_services limit 20'
        
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
     
     #write to input file
     query_results | 'Write log input' >> WriteToText('input.txt')
        
     # apply ParDo to format the data 
     formatted_data_pcoll = query_results | 'Format Data' >> beam.ParDo(FormatDataFn())
        
     # write PCollection to log file
     formatted_data_pcoll | 'Write log 1' >> WriteToText('output.txt')
        
    
    
     dataset_id = 'samhda_modeled'
     table_id = 'facility_services_Beam'
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


