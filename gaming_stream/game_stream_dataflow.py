################################################################################################################
#
#   Google Cloud Dataflow
#
#   References:
#   https://cloud.google.com/dataflow/docs/
#
#   Usage:
'''
python3 game_stream_dataflow.py \
    --gcp_project remy-sandbox \
    --region us-central1 \
    --job_name 'gamelogs3' \
    --gcp_staging_location "gs://dataflow-rw/stream-staging" \
    --gcp_tmp_location "gs://dataflow-rw/stream-staging/tmp" \
    --batch_size 10 \
    --input_topic projects/remy-sandbox/topics/game-logs \
    --bq_dataset_name game_stream \
    --bq_table_name game_stream \
    --runner DataflowRunner \
    --testarg nothing \
    --numWorkers 4 \
    --autoscalingAlgorithm NONE
     #--runner DataflowRunner &


     next:figure out how to run with args

Steps to get it to run:

1.If you need to setup from scratch, use the setup.sh here to create pubsub/bq table in project, then install packages on your own machine
2.Run the dataflow script to start the job from bash using the arguments at the top of the script - /Users/remyw/Documents/Code Projects/Twitter stream/twitter_workspace/twitter_streaming/game_stream_dataflow.py
3.Wait a few minutes for it to get up and running
4.Run the stream_game_events.py script from VS Code. This will generate 100 events to pubsub, which will then get picked up by the dataflow job

'''
#
################################################################################################################


from __future__ import absolute_import
import os, sys
import logging
import argparse
import json
import apache_beam as beam
from apache_beam import window
from apache_beam.transforms import trigger
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from past.builtins import unicode

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "
################################################################################################################
#
#   Variables
#
################################################################################################################
'''NEXT:
created VM with python and git on it to run this code on.
'''
bq_schema = {'fields': [
    {'name': 'uid',         'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'game_id',     'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'game_server', 'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'game_type',   'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'game_map',    'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'event_datetime',   'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'player',      'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'killed',      'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'weapon',      'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'x_cord',      'type': 'INT64',  'mode': 'NULLABLE'},
    {'name': 'y_cord',      'type': 'INT64',  'mode': 'NULLABLE'}
]}

################################################################################################################
#
#   Functions
#
################################################################################################################

def parse_pubsub(line):
    return json.loads(line)


def extract_map_type(event):
    return event['game_map']

def sum_by_group(GroupByKey_tuple):
      (word, list_of_ones) = GroupByKey_tuple
      return {"word":word, "count":sum(list_of_ones)}

def run(argv=None):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--gcp_project',          required=True,  default='remy-sandbox',                   help='GCP Project ID')
    parser.add_argument('--region',               required=True,  default='us-central1',                   help='GCP Project ID')
    parser.add_argument('--job_name',             required=True,  default='gamelogs3',                   help='Dataflow Job Name')
    parser.add_argument('--gcp_staging_location', required=True,  default='gs://dataflow-rw/stream-staging', help='Dataflow Staging GCS location')
    parser.add_argument('--gcp_tmp_location',     required=True,  default='gs://dataflow-rw/stream-staging/tmp',     help='Dataflow tmp GCS location')
    parser.add_argument('--batch_size',           required=True,  default=10,                   help='Dataflow Batch Size')
    parser.add_argument('--input_topic',          required=True,  default='projects/remy-sandbox/topics/game-logs',                   help='Input PubSub Topic: projects/<project_id>/topics/<topic_name>')
    parser.add_argument('--bq_dataset_name',      required=True,  default='game_stream',                   help='Output BigQuery Dataset')
    parser.add_argument('--bq_table_name',        required=True,  default='game_stream',                   help='Output BigQuery Table')
    parser.add_argument('--runner',               required=True,  default='DirectRunner',       help='Dataflow Runner - DataflowRunner or DirectRunner (local)')
    parser.add_argument('--numWorkers',               required=True,  default='3',       help='numworkers')
    parser.add_argument('--autoscalingAlgorithm',               required=True,  default='NONE',       help='autoscaling default is None')

    known_args, pipeline_args = parser.parse_known_args(argv)
    
    pipeline_args.extend([
          '--runner={}'.format(known_args.runner),                          # DataflowRunner or DirectRunner (local)
          '--project={}'.format(known_args.gcp_project),
          '--staging_location={}'.format(known_args.gcp_staging_location),  # Google Cloud Storage gs:// path
          '--temp_location={}'.format(known_args.gcp_tmp_location),         # Google Cloud Storage gs:// path
          '--job_name=' + str(known_args.job_name),
          '--region={}'.format(known_args.region),
      ])
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    
    ###################################################################
    #   DataFlow Pipeline
    ###################################################################
    
    with beam.Pipeline(options=pipeline_options) as p:
        
        logging.info('Ready to process events from PubSub topic: {}'.format(known_args.input_topic))
        
        # Read the pubsub topic into a PCollection.
        events = ( 
                 p  | beam.io.ReadFromPubSub(known_args.input_topic) 
        )
        
        # Parse events
        parsed = (
            events  | beam.Map(parse_pubsub)
        )
        
        # Tranform events
        transformed = (
            parsed  | beam.Map(extract_map_type)
                    | beam.Map(lambda x: (x, 1))
                    | beam.WindowInto(window.SlidingWindows(30, 5)) # Window is 30 seconds in length, and a new window begins every five seconds
                    | beam.GroupByKey()
                    | beam.Map(sum_by_group)
        )
        
        # Print results to console (for testing/debugging)
        transformed | 'Print aggregated game logs' >> beam.Map(print)
        
        # Sink/Persist to BigQuery
        parsed | 'Write to bq' >> beam.io.gcp.bigquery.WriteToBigQuery(
                        table=known_args.bq_table_name,
                        dataset=known_args.bq_dataset_name,
                        project=known_args.gcp_project,
                        schema=bq_schema,
                        batch_size=int(known_args.batch_size)
                        )
        
        # Sink data to PubSub
        #output | beam.io.WriteToPubSub(known_args.output_topic)


################################################################################################################
#
#   Main
#
################################################################################################################

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()



'''
python ./stream_game_events.py --project_id gaming-demos --bq_dataset_id streaming --bq_table_id game_logs --pubsub_topic game-logs --sink pubsub --number_of_records 10 --delay 2
'''


