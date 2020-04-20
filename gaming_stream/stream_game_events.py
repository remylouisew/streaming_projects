
'''
USAGE:
python3 ./stream_game_events.py --project_id remy-sandbox --bq_dataset_id game_stream --bq_table_id game_stream --pubsub_topic game-logs --sink pubsub --number_of_records 10 --delay 2
'''

import os,sys,csv
import json
import random
from random_username.generate import generate_username
import datetime,time
import argparse
import subprocess
from google.cloud import bigquery, pubsub_v1

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""

#########################################################
#
#   Functions
#
#########################################################

def sim( bias ):
    
    # Game Server
    game_server = random.choice( game_servers )
    
    # Game Type
    #game_type  = game_types[ int(random.triangular(0, len(game_types), int(len(game_types)*bias) )) ]
    game_type   = game_types[ int(len(game_types)*bias) ] if random.random() <= 0.75 else random.choice( game_types )
    
    # Game Map
    #game_map   = game_maps[ int(random.triangular(0, len(game_maps), int(len(game_maps)*bias) )) ]
    game_map    = game_maps[ int(len(game_maps)*bias) ] if random.random() <= 0.75 else random.choice( game_maps )
    
    # Player (Killer)
    player     = players[ int(random.triangular(0, len(players), int(len(players)*bias) )) ]
    
    # Player (Killed)
    killed      = players[ int(random.triangular(0, len(players), len(players)-10 )) ]
    
    # Coorinates
    x_cord = int(random.triangular(0,99, int(99*bias) - random.randint(1,20) ))
    x_cord = x_cord if x_cord >= 0 else int(random.triangular(0,99, int(99*bias) ))
    y_cord = int(random.triangular(0,99, int(99*bias) ))
    
    # Datetime
    year   = '2019'
    month  = str(random.randint(1,4)).zfill(2)
    day    = str(random.randint(1,28)).zfill(2)
    #hour  = str(int(random.triangular(0, 23, int(24*bias) ))).zfill(2)
    hour   = str(int(24*bias)).zfill(2) if random.random() <= 0.70 else str(int(random.randint(0, 23 ))).zfill(2)
    minute = str(random.randint(1,59)).zfill(2)
    second = str(random.randint(1,59)).zfill(2)
    event_datetime = '{}-{}-{} {}:{}:{}'.format(year,month,day,hour,minute,second)
    
    return game_server, game_type, game_map, player, killed, x_cord, y_cord, event_datetime


def pubsub_publish( pubsub_publisher, project_id, pubsub_topic, message ):
    '''
        Pub/Sub Publish Message
        Notes:
          - When using JSON over REST, message data must be base64-encoded
          - Messages must be smaller than 10MB (after decoding)
          - The message payload must not be empty
          - Attributes can also be added to the publisher payload
        
        
        pubsub_publisher  = pubsub_v1.PublisherClient()
        
    '''
    try:
        # Initialize PubSub Path
        pubsub_topic_path = pubsub_publisher.topic_path( project_id, pubsub_topic )
        
        # If message is JSON, then dump to json string
        if type( message ) is dict:
            message = json.dumps( message )
        
        # When you publish a message, the client returns a Future.
        #message_future = pubsub_publisher.publish(pubsub_topic_path, data=message.encode('utf-8'), attribute1='myattr1', anotherattr='myattr2')
        message_future = pubsub_publisher.publish(pubsub_topic_path, data=message.encode('utf-8') )
        message_future.add_done_callback( pubsub_callback )
    except Exception as e:
        print('[ ERROR ] {}'.format(e))


def pubsub_publish_bash( topic_name, json_message ):
    '''
        Publishs a JSON message to Google PubSub (Bash method if client lib not working)
    '''
    json_str = json.dumps( json_message )
    output   = subprocess.Popen( ['gcloud', 'pubsub', 'topics', 'publish', topic_name, '--message', json_str] )
    return None


def pubsub_callback( message_future ):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('[ ERROR ] Publishing message on {} threw an Exception {}.'.format(topic_name, message_future.exception()))
    else:
        print('[ INFO ] Result: {}'.format(message_future.result()))


def stream_to_bq( bq_client, bq_table, json_payload ):
    '''
        bq_client    = bigquery.Client()
        bq_table_ref = client.dataset(bq_dataset_id).table(bq_table_id)
        bq_table     = client.get_table(table_ref)
    '''
    errors    = bq_client.insert_rows(bq_table, [tuple([ v for k,v in json_payload.items() ])] )
    if errors == []:
        print('[ INFO ] Complete. Successfully inserted records into BigQuery')
    else:
        print('[ WARNING ] Failed to write records to BigQuery')
    
    return None


#########################################################
#
#   Constants (used for the simulation)
#
#########################################################

game_servers = [
'[WTWRP] Deathmatch',
'[WTWRP] Votable',
'Jeffs Vehicle Warfare',
'(SMB) Kansas Public [git]',
'exe.pub | Relaxed Running | CTS/XDF',
'Corcs do Harleys Xonotic Server',
'Jeff & Julius Resurrection Server',
'Odjel za Informatikus Xonotic Server',
'[PAC] Pickup'
]

game_types = [
'Keyhunt',
'Clan Area',
'Deathmatch',
'Capture The Flag',
'Team Death Match',
'Complete This Stage'
]

game_maps = [
'boil',
'atelier',
'implosion',
'finalrage',
'afterslime',
'solarium',
'xoylent',
'darkzone',
'warfare',
'stormkeep'
]

weapons =   10*['Electro'] + \
             8*['Hagar'] + \
            10*['Shotgun'] + \
            10*['Mine Layer'] + \
             8*['Crylink'] + \
            10*['Mortar'] + \
            15*['Blaster'] + \
            20*['Machine Gun'] + \
            10*['Devastator'] + \
            10*['Vortex']

weapons =   1*['Electro'] + \
            1*['Hagar'] + \
            1*['Shotgun'] + \
            1*['Mine Layer'] + \
            1*['Crylink'] + \
            1*['Mortar'] + \
            1*['Blaster'] + \
            1*['Machine Gun'] + \
            1*['Devastator'] + \
            1*['Vortex']


players = generate_username( 5000 )


#########################################################
#
#   Main
#
#########################################################

if __name__ == "__main__":
    
    ap = argparse.ArgumentParser()
    ap.add_argument("--project_id",         required=True,               help="GCP Project ID")
    ap.add_argument("--bq_dataset_id",      required=True,               help="BigQuery Dataset ID")
    ap.add_argument("--bq_table_id",        required=True,               help="BigQuery Table Name")
    ap.add_argument("--pubsub_topic",       required=False,              help="BigQuery Table Name")
    ap.add_argument("--sink",               required=True,               help="Set to 'pubsub' or 'bigquery'")
    ap.add_argument("--number_of_records",  required=False, default=100, help="Number of records to stream")
    ap.add_argument("--delay",              required=False, default=0,   help="Time delay inbetween events")
    args = vars(ap.parse_args())
    
    '''
    # Used for Testing Only
    args = {
        'project_id':         'zproject201807',
        'bq_dataset_id':      'gaming',
        'bq_table_id':        'gaming_events_stream',
        'pubsub_topic':       'gaming_events',
        'sink':               'pubsub',
        'number_of_records':  10,
        'delay':              2
    }
    '''
    
    # Initialize BigQuery Object
    bq_client     = bigquery.Client()
    bq_table_ref  = bq_client.dataset(args['bq_dataset_id']).table(args['bq_table_id'])
    bq_table      = bq_client.get_table(bq_table_ref)
    
    # Initialize PubSub Object
    pubsub_publisher  = pubsub_v1.PublisherClient()
    #pubsub_topic_path = pubsub_publisher.topic_path(args['project_id'], args['pubsub_topic'])
    
    # Generate initial game_id as a starting point
    game_id = '{}-{}-{}{}'.format( generate_username( 1 )[0], int(random.random()*10000000), int(random.random()*100000000000000), int(random.random()*100000000000000) )

    for i in range( int(args['number_of_records']) ):

        # UID
        uid = '{}_{}'.format(datetime.datetime.now().strftime('%Y%m%d_%H%M%S%f'), int(random.random()*10000) )

        # Game ID
        if random.random()>=0.98:
            game_id = '{}-{}-{}{}'.format( generate_username( 1 )[0], int(random.random()*10000000), int(random.random()*100000000000000), int(random.random()*100000000000000) )
        else:
            game_id = game_id

        # Weapon
        weapon = random.choice( weapons )

        if   weapon=='Electro':     bias=0.05
        elif weapon=='Hagar':       bias=0.15
        elif weapon=='Shotgun':     bias=0.25
        elif weapon=='Mine Layer':  bias=0.35
        elif weapon=='Crylink':     bias=0.45
        elif weapon=='Mortar':      bias=0.55
        elif weapon=='Blaster':     bias=0.65
        elif weapon=='Machine Gun': bias=0.75
        elif weapon=='Devastator':  bias=0.85
        elif weapon=='Vortex':      bias=0.95
        else:                       bias=0.50

        game_server, game_type, game_map, player, killed, x_cord, y_cord, event_datetime = sim( bias )

        # Stream to BigQuery
        payload = {
            'uid':          uid,
            'game_id':      game_id,
            'game_server':  game_server,
            'game_type':    game_type,
            'game_map':     game_map,
            'event_datetime': event_datetime,
            'player':       player,
            'killed':       killed,
            'weapon':       weapon,
            'x_cord':       x_cord,
            'y_cord':       y_cord
        }

        # Write to sink - Either PubSub or directly to BigQuery
        if args['sink'] == 'pubsub':
            pubsub_publish( pubsub_publisher, project_id=args['project_id'], pubsub_topic=args['pubsub_topic'], message=payload )
            #pubsub_publish_bash( topic_name=args['pubsub_topic'], json_message=payload )
        elif args['sink'] == 'bigquery':
            stream_to_bq( bq_client, bq_table, json_payload=payload )
        else:
            print('[ ERROR ] No value set for "sink"')
            sys.exit()

        time.sleep( float(args['delay']) )
        print(payload)



#ZEND
