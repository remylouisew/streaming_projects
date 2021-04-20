Steps to get it to run:

1) If you need to setup from scratch, use the setup.sh to create pubsub + bq tables in your project, then install packages on your own machine/ VM
2) Run the command at the top of game_stream_dataflow.py script to start the dataflow job 
3) Wait a few minutes for it to get up and running
4) Run the stream_game_events.py script. This will generate 100 events to pubsub, which will then get picked up by the dataflow job

*Make sure you change Project ID, etc in both setup.sh, and the script execution commands!
