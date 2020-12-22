# Repository for my Master Thesis at Politecnico di Milano
## "A comparative study on parallelization of streaming top-k algorithms in Kafka"

## How to try it!

Run `docker-compose up -d` to build and run the images

Run the application using its jar file in `out/artifacts/APP_NAME/`

Run `src/main/java/myapp/RatingsDriverTest.java`, change the `INPUT_TOPIC` in the file based on the app you're testing.

## Measurements

### Centralized Benchmark
Run `RatingsDriver` with `msstopk-scored-rated-movies-${DATASET}`, `INPUT_THROUGHPUT` and `DATASET` as arguments.

Then:
Run `src/main/java/myapp/materializeScoreSort/PhysicalWindow/CentralizedMSSTopK.java` with `TOPK` and `DATASET` as arguments

Check when reading offset << writing offset

Run `cleanLatencyFile.py CentralizedMSSTopK/dataset/topKK_latency_5s.txt TOPK` to get the final `MaterializeSort/latency_5ms.csv`

Run `averageLatency.py CentralizedMSSTopK/dataset/topKK_latency_5s.txt` to get average Latency of the experiment.

---

### Centralized MinTopK
Run `RatingsDriver` with `centralized-mintopk-scored-rated-movies-${DATASET}`, `INPUT_THROUGHPUT` and `DATASET` as arguments.

Then:

Run the experiment using the 5 datasets that you can find in `dataset/`

Run `java -jar out/artifacts/minTopK_jar/kafka-stream-tutorial.jar "src/main/java/myapp/minTopK/minTopK.env"  topK dataset` where topK and dataset are integer values.

Run `cleanLatencyFile.py CentralizedMinTopK/topKK_latency_5ms.txt topK` to get the final `CentralizedMinTopK/topKK_latency_5ms.csv`

Run `averageLatency.py CentralizedMinTopK/topKK_latency_5ms.txt` to get average Latency of the experiment.

---

### Centralized MinTopK+N

Run the experiment using the 5 datasets that you can find in `dataset/`

Run `java -jar out/artifacts/CentralizeMinTopKN_jar/CentralizeMinTopKN.jar "src/main/java/myapp/minTopKN/minTopKN.env"  topK topN dataset` where topK, topN and dataset are integer values.

Run `RatingsDriver` with `centralized-mintopkn-scored-rated-movies`, `INPUT_THROUGHPUT` and `DATASET` as arguments.

Run `cleanLatencyFile.py CentralizedMinTopKN/topKK_latency_5ms.txt topK` to get the final `CentralizedMinTopKN/topKK_latency_5ms.csv`

Run `averageLatency.py CentralizedMinTopKN/topKK_latency_5ms.txt` to get average Latency of the experiment.

---

### Parallel Benchmark
Load input data running `RatingsDriver` with `pdmss-scored-rated-movies-dataset${DATASET}`, `INPUT_THROUGHPUT` and `DATASET` as arguments.

Then:

Run 3 instances of `src/main/java/myapp/distributedMaterializeScoreSort/PhysicalWindow/PhysicalWindowDistributedMSS ENV_FILE TOPK DATASET #INSTANCE`.

Run `src/main/java/myapp/distributedMaterializeScoreSort/PhysicalWindow/PhysicalWindowCentralizedAggregatedSort ENV_FILE TOPK DATASET`.

Run both files with `src/main/java/myapp/distributedMaterializeScoreSort/PhysicalWindow/physicalWindowDisMSS.env` as `ENV_FILE` argument.

---

### Parallel MinTopK
Run `RatingsDriver` with `dis-mintopk-scored-rated-movies-dataset${DATASET}`, `INPUT_THROUGHPUT` and `DATASET` as arguments.

Then:

Run 3 instances of `src/main/java/myapp/distributedMinTopK/DistributedMinTopK.java ENV_FILE TOPK DATASET #INSTANCE`.

Run `src/main/java/myapp/distributedMinTopK/CentralizedTopK.java ENV_FILE TOPK DATASET DisMinTopK`.

Run both files with `src/main/java/myapp/distributedMinTopK/disMinTopK.env` as first argument.

---
### WIP: Parallel MinTopK+N

Run 3 instances of `src/main/java/myapp/distributedMinTopKN/DistributedMinTopKN.java ENV_FILE TOPK TOPN DATASET #INSTANCE`.

Run `src/main/java/myapp/distributedMinTopK/CentralizedTopK.java ENV_FILE TOPK DATASET DisMinTopKN`.

Run both files with `src/main/java/myapp/distributedMinTopKN/disMinTopKN.env` as first argument.

Run `RatingsDriver` with `dis-mintopkn-scored-rated-movies`, `INPUT_THROUGHPUT` and `DATASET` as arguments.

---
#### Parallel measurements
Run `./measurements.sh NUM_INSTANCES DATASET ALGO TOPK` to clean measurent files and compute distributed latency and total_time 
ALGO parameter can be one betweem `DisMSSTopK` and `DisMinTopK` \
(For NUM_INSTANCES != 6 need to modify python script `distributedLatency.py`)

---
### Average latency

Run `python3 averageLatency.py CentralizedMinTopK/dataset0/500Krecords_1200_300_50K_latency_5s.csv CentralizedMinTopK/dataset1/500Krecords_1200_300_50K_latency_5s.csv CentralizedMinTopK/dataset2/500Krecords_1200_300_50K_latency_5s.csv CentralizedMinTopK/dataset3/500Krecords_1200_300_50K_latency_5s.csv CentralizedMinTopK/dataset4/500Krecords_1200_300_50K_latency_5s.csv`

---

### Mean and std

##### Mean and std per dataset

Run `python3 measurementsPerDataset.py CentralizedMinTopK/dataset0/500Krecords_1200_300_50K_latency_5s.csv CentralizedMinTopK/dataset1/500Krecords_1200_300_50K_latency_5s.csv CentralizedMinTopK/dataset2/500Krecords_1200_300_50K_latency_5s.csv CentralizedMinTopK/dataset3/500Krecords_1200_300_50K_latency_5s.csv CentralizedMinTopK/dataset4/500Krecords_1200_300_50K_latency_5s.csv`

##### Mean and std per topK

Run `python3 measurements.py CentralizedMinTopK/dataset0/500Krecords_1200_300_2K_average.csv CentralizedMinTopK/dataset0/500Krecords_1200_300_10K_average.csv CentralizedMinTopK/dataset0/500Krecords_1200_300_50K_average.csv`

---

### Plot box plot 
##### Latency Box plot per dataset with same topK

Run `python3 latencyBoxPlotPerDataset.py CentralizedMinTopK/dataset0/500Krecords_1200_300_2K_latency_5s.csv CentralizedMinTopK/dataset1/500Krecords_1200_300_2K_latency_5s.csv CentralizedMinTopK/dataset2/500Krecords_1200_300_2K_latency_5s.csv CentralizedMinTopK/dataset3/500Krecords_1200_300_2K_latency_5s.csv CentralizedMinTopK/dataset4/500Krecords_1200_300_2K_latency_5s.csv TITLE`

##### Latency Box plot per topK

Run `python3 plotBoxPlot.py CentralizedMinTopK/dataset0/500Krecords_1200_300_2K_average.csv CentralizedMinTopK/dataset0/500Krecords_1200_300_10K_average.csv CentralizedMinTopK/dataset0/500Krecords_1200_300_50K_average.csv TITLE`

##### Total Time Box plot per Algorithm

Run `python3 totalTimeBoxPlotPerAlgo` with the `total_times_6instances.csv` of each algorithms

##### Total Time Box plot per Number of Instances

Run `python3 totalTimeBoxPlotPerInstances total_times_3instances.csv total_times_6instances.csv total_times_10instances.csv`

##### Total Time Box plot per Top-K

Run `python3 totalTimeBoxPlotPerTopK` with the `total_times_6instances.csv` for each topk. 
