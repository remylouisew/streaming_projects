
Steps to set up the streaming job in your own project:

1) If you need to setup from scratch, use the setup.sh to create pubsub + bq tables in your project, then install packages on your own machine/ VM
2) Run the command at the top of game_stream_dataflow.py script to start the dataflow job 
3) Wait a few minutes for it to get up and running
4) Run the stream_game_events.py script. This will generate X events to pubsub, which will then get picked up by the dataflow job, and the records will be written to BQ

How to generate events in our colab project:

1) SSH into the VM game-stream 
2) cd streaming_projects/mlops_version
3) Run the "bq mk...." statement in setup.sh to create your own table in bigquery (obviously change the name). You shouldn't have to run any other part of setup.sh
4) Run the command at the top of the stream_game_events.py script **be sure to change the bq table argument to your own table!   
        - You may need to run 'gcloud auth application-default login' before running the script 
5) Change the following arguments to match what you need:  
 --number_of_records 10000 = 10,000 total records will be generated  
 --delay 0.0001 = 1000 records per sec  


