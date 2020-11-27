# Distributed materialize score and sort

## How to try it

### Distributed scoring, centralized sorting
Run as many instances of `DistributedMaterializeScore` as you want.

Run `CentralizedSorting`  with `"dmss.env"` as argument.

output topic: `dmss-sorted-rated-movies`

### Distributed sorting, centralized aggregation and topK 
Run as many instances of `DistributedMaterializeScoreSort` as you want.

Run `CentralizedAggregatedSort`  with `"dmss.env"` as argument.

output topic: `dmss-topk-rated-movie`

### Physical Window: Distributed sorting, centralized aggregation
Run 3 instances of `PhysicalWindowDistributedMSS ENV_FILE TOPK DATASET INSTANCE`.

Run `PhysicalWindowCentralizedAggregatedSort ENV_FILE TOPK DATASET`.

Run both files with `physicalWindowDisMSS.env` as argument.

output topic: `pdmss-final-sorted-rated-movies`

### Measurements 

Run `cleanEndTimeFile.py PhysicalDisMaterializeSort/dataset0/500Krecords_1200_300_end_time_5ms.txt -1` to get`DisMinTopKN/500Krecords_1200_300_topKK_end_time_5ms.csv`

Run `cleanStartTimeFile.py PhysicalDisMaterializeSort/dataset0/instance0_500Krecords_1200_start_time_5ms.txt LocalWindowSize LocalWindowHoppingSize` for each instance's file

Run `distributedLatency.py instance0.csv instance1.csv instance2.csv endTime.csv` to get the final latency for each windows

Run `plotLatency.py PhysicalDisMaterializeSort/dataset0/500Krecords_1200_300_5ms.csv` to plot the results
