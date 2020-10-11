#!/usr/bin/env bash

for i in $(eval echo {0..$1})
  do
      python3 pythonScripts/getStartingWindowTime.py "measurements/DisMSSTopK/dataset${2}/instance${i}_100Krecords_3600_300_10K_start_time_6instances.txt" 50
  done
python3 pythonScripts/cleanEndTimeFile.py "measurements/DisMSSTopK/dataset${2}/100Krecords_3600_300_10K_end_time_6instances.txt" 10
python3 pythonScripts/totalTime.py "measurements/DisMSSTopK/dataset${2}/instance0_100Krecords_3600_300_10K_start_time_6instances_start_window.csv" "measurements/DisMSSTopK/dataset${2}/100Krecords_3600_300_10K_end_time_6instances.csv"
python3 pythonScripts/distributedLatency.py 6 "measurements/DisMSSTopK/dataset${2}/100Krecords_3600_300_10K_end_time_6instances.csv" "measurements/DisMSSTopK/dataset${2}/instance0_100Krecords_3600_300_10K_start_time_6instances_start_window.csv" "measurements/DisMSSTopK/dataset${2}/instance1_100Krecords_3600_300_10K_start_time_6instances_start_window.csv" "measurements/DisMSSTopK/dataset${2}/instance2_100Krecords_3600_300_10K_start_time_6instances_start_window.csv" "measurements/DisMSSTopK/dataset${2}/instance3_100Krecords_3600_300_10K_start_time_6instances_start_window.csv" "measurements/DisMSSTopK/dataset${2}/instance4_100Krecords_3600_300_10K_start_time_6instances_start_window.csv" "measurements/DisMSSTopK/dataset${2}/instance5_100Krecords_3600_300_10K_start_time_6instances_start_window.csv"
