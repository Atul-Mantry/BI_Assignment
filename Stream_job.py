# standard libs
import argparse
import json
from datetime import datetime
import time
import logging
from time import gmtime, strftime

# beam libs
import apache_beam as beam
from apache_beam import DoFn, io, ParDo, Pipeline
from apache_beam.io.external import kafka
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions



logging.basicConfig(level=logging.DEBUG)

def cleandatetime(x):
  """Return datetime for epoch datetime fields"""
  if x != 'None' and x is not None and x != 'null':    return time.strftime ( '%Y-%m-%d %H:%M:%S', time.gmtime ( float(x) / 1000.0 ) )
  else:
    return None

class cleandata ( beam.DoFn ):
  """Class built to clean data in parallel line by line"""
    
  def process(self, element):
    """function to clean and return data"""
	
	# dependencies for parallen processing	
    import pandas as pd

    # fetching current datetime
    t = strftime ("%Y-%m-%d %H:%M:%S",gmtime())

    # defining new dicts
    e = {}  # to capture created at
    c = {}  # to capture type

    # new dict to add current datetime
    e['createdat'] = t

    # parse: str to dict
    a = json.loads(element)

    b = a['type']   # pass type to var b
    c['type'] = b   # pass type to temp dict

    p = a['body'] # pass body to p
    p.update(c) # add type to body
    p.update(e) # add created datetime to body

    # clean submitted date
    if 'submittedDate' in p:
      p['submittedDate'] = cleandatetime ( p['submittedDate'] )

    # clean cancel requested date
    if 'cancelRequestedDate' in p:
      p['cancelRequestedDate'] = cleandatetime ( p['cancelRequestedDate'] )

    # clean cancelled date
    if 'cancelledDate' in p:
      p['cancelledDate'] = cleandatetime ( p['cancelledDate'] )
	  
	# clean filled date
    if 'filledDate' in p:
      p['filledDate'] = cleandatetime ( p['filledDate'] )

    # clean execution time
    if 'executionTime' in p:
      p['executionTime'] = cleandatetime ( p['executionTime'] )

    # clean expire date
    if 'expireDate' in p:
      p['expireDate'] = cleandatetime ( p['expireDate'] )

    # clean expire date
    if 'expireTime' in p:
      p['expireTime'] = cleandatetime ( p['expireTime'] )

    # clean settlement Date
    if 'settlementDate' in p:
      p['settlementDate'] = cleandatetime ( p['settlementDate'] )

    # clean trade Date (list to date)
    if 'tradeDate' in p:
      p['tradeDate'] = datetime.strptime ( '-'.join ( str ( x ) for x in p['tradeDate'] ), '%Y-%m-%d' ).strftime (
        "%Y-%m-%d" )

    # splits execution window in to and from cols
    if 'executionWindow' in p:
      p['executionwindowfrom'] = cleandatetime (p['executionWindow'][:13])
      p['executionwindowto'] = cleandatetime (p['executionWindow'][14:])
      del p['executionWindow']

	# flattening the json
    df = pd.json_normalize(p, sep='_')
	
	# flat json to flat dict
    p = df.to_dict(orient='records')[0]

    return [p] 
	  
class WriteDataframeToBQ(beam.DoFn):

    def __init__(self, bq_dataset,  project_id):
        self.bq_dataset = bq_dataset
       # self.bq_table = bq_table
        self.project_id = project_id

    def process(self, element):
	
	    # dependencies for parallen processing	
        from google.cloud import bigquery

		# establishing connection to bq
        self.client = bigquery.Client()

		# creating copy of elemnt
        y = element
		
		# capture table name
        x = y['type']

        # table where we're going to store the data
        table_id = f"{self.bq_dataset}.{x}"

        # Conver to ndjson               
        y = [y]

		# bq job config
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,            
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ],
			time_partitioning=bigquery.table.TimePartitioning(field="createdat"),    # use day partition
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND   				 # always append the data
			)

        # try loading data
        try:
            load_job = self.client.load_table_from_json(
                y,
                table_id,
                job_config=job_config,
            )  # Make an API request.

            load_job.result()  # Waits for the job to complete.
            if load_job.errors:
                logging.info(f"error_result =  {load_job.error_result}")
                logging.info(f"errors =  {load_job.errors}")
            else:
                logging.info(f'Loaded {len(element)} rows.')

        except Exception as error:
            logging.info(f'Error: {error} with loading data')


# job input params			
parser = argparse.ArgumentParser(description='Json to BigQuery')
parser.add_argument('--project',required=True, help='Specify Google Cloud project')
parser.add_argument('--region', required=True, help='Specify Google Cloud region')
parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
parser.add_argument('--bigquery_dataset', required=True, help='Bigquery Dataset to write data')
parser.add_argument('--in_gcs_file', required=True, help='Bigquery Dataset to write data')
parser.add_argument('--job_name', required=False, help='Bigquery Dataset to write data')  # pass job name when update required

# passing argument
opts = parser.parse_args()

# Setting up the Beam pipeline options
options = PipelineOptions(save_main_session=True, streaming=True)  # add update=True
options.view_as(GoogleCloudOptions).project = opts.project
options.view_as(GoogleCloudOptions).region = opts.region
options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
options.view_as(StandardOptions).runner = opts.runner

# set input file path
input = opts.in_gcs_file


# beam pipeline
with beam.Pipeline(options=options) as pipeline:
  x = (
      pipeline
	  #| "Read Kafka Messages" >> kafka.ReadFromKafka({'bootstrap.servers': ['10.164.0.4:9094','10.164.0.3:9094']},'UserData') #doesn't work
      | 'Read to GCS' >> beam.io.ReadFromText(input)    #takes data from the file
      | 'Parse Json' >> beam.Map(lambda line: json.loads(json.dumps(line)))  #parse the data
      | 'Clean data' >> beam.ParDo(cleandata()) #clean the data ||
      #| 'Write to GCS' >> beam.io.WriteToText(output) #write processed data to gcs
      | "Write to Big Query" >> beam.ParDo(WriteDataframeToBQ(project_id=opts.project, bq_dataset=opts.bigquery_dataset)) #write to bq ||
      )
	