## MLOPS study group, use the scripts in the MLOPS_Version folder, not this one

Steps to set up the streaming job in your own project:

1) If you need to setup from scratch, use the setup.sh to create pubsub + bq tables in your project, then install packages on your own machine/ VM
2) Run the command at the top of game_stream_dataflow.py script to start the dataflow job 
3) Wait a few minutes for it to get up and running
4) Run the stream_game_events.py script. This will generate X events to pubsub, which will then get picked up by the dataflow job, and the records will be written to BQ

How to generate events:

1) Change BQ table to your own table 
2) Run the stream_game_events.py script.
3) Change the following arguments to match what you need:
 --number_of_records 10000 = 10,000 total records will be generated
 --delay 0.0001 = 1000 records per sec

*Make sure you change Project ID, etc in both setup.sh, and the script execution commands!
