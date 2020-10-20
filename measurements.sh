#!/usr/bin/env bash
for i in $(eval echo {0..$(($1 - 1))})
  do
      python3 pythonScripts/getStartingWindowTime.py "measurements/${3}/top${4}/dataset${2}/instance${i}_100Krecords_3600_300_${4}K_start_time_${1}instances.txt" $((300/${1}))
  done
python3 pythonScripts/cleanEndTimeFile.py "measurements/${3}/top${4}/dataset${2}/100Krecords_3600_300_${4}K_end_time_${1}instances.txt" ${4}
python3 pythonScripts/totalTime.py "measurements/${3}/top${4}/dataset${2}/instance0_100Krecords_3600_300_${4}K_start_time_${1}instances_start_window.csv" "measurements/${3}/top${4}/dataset${2}/100Krecords_3600_300_${4}K_end_time_${1}instances.csv"

files=()

for j in $(eval echo {0..$(($1 - 1))})
  do
    files+=("measurements/${3}/top${4}/dataset${2}/instance${j}_100Krecords_3600_300_${4}K_start_time_${1}instances_start_window.csv")
  done
python3 pythonScripts/distributedLatency.py ${1} "measurements/${3}/top${4}/dataset${2}/100Krecords_3600_300_${4}K_end_time_${1}instances.csv" ${files[@]}

#"measurements/${3}/top${4}/dataset${2}/instance0_100Krecords_3600_300_${4}K_start_time_6instances_start_window.csv" "measurements/${3}/top${4}/dataset${2}/instance1_100Krecords_3600_300_${4}K_start_time_6instances_start_window.csv" "measurements/${3}/top${4}/dataset${2}/instance2_100Krecords_3600_300_${4}K_start_time_6instances_start_window.csv" "measurements/${3}/top${4}/dataset${2}/instance3_100Krecords_3600_300_${4}K_start_time_6instances_start_window.csv" "measurements/${3}/top${4}/dataset${2}/instance4_100Krecords_3600_300_${4}K_start_time_6instances_start_window.csv" "measurements/${3}/top${4}/dataset${2}/instance5_100Krecords_3600_300_${4}K_start_time_6instances_start_window.csv"
# "measurements/${3}/dataset${2}/instance6_100Krecords_3600_300_10K_start_time_6instances_start_window.csv" "measurements/${3}/dataset${2}/instance7_100Krecords_3600_300_10K_start_time_6instances_start_window.csv" "measurements/${3}/dataset${2}/instance8_100Krecords_3600_300_10K_start_time_6instances_start_window.csv" "measurements/${3}/dataset${2}/instance9_100Krecords_3600_300_10K_start_time_6instances_start_window.csv"
