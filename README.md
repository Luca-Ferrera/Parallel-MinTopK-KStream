# Master thesis on AquaTopK using Kafka stream

## Examples of Data

### Movies
```
{"id": 294, "title": "Die Hard", "release_year": 1988}
{"id": 354, "title": "Tree of Life", "release_year": 2011}
{"id": 782, "title": "A Walk in the Clouds", "release_year": 1995}
{"id": 128, "title": "The Big Lebowski", "release_year": 1998}
{"id": 100, "title": "Spiderman", "release_year": 2002}
{"id": 120, "title": "Pirates of The Caribbean", "release_year": 2003}
{"id": 140, "title": "La Grande Bellezza", "release_year": 2013}
```

### Ratings
```
{"id": 294, "rating": 8.2}
{"id": 294, "rating": 8.5}
{"id": 354, "rating": 9.9}
{"id": 354, "rating": 9.7}
{"id": 782, "rating": 7.8}
{"id": 782, "rating": 7.7}
{"id": 128, "rating": 8.7}
{"id": 128, "rating": 8.4}
{"id": 780, "rating": 2.1}
{"id": 100, "rating": 6.3}
{"id": 120, "rating": 7.2}
{"id": 140, "rating": 4.5}
{"id": 120, "rating": 8.9}
```
## How to try it!

Run `docker-compose up -d` to build and run the images

Run the application using its jar file in `out/artifacts/APP_NAME/`

Run `src/main/java/myapp/RatingsDriverTest.java`, change the `INPUT_TOPIC` in the file based on the app you're testing.

## Measurements

### Centralized Benchmark
Run `MaterializeScoreSort`

Run `RatingsDriver` with `scored-rated-movies`, `INPUT_THROUGHPUT` and `DATASET` as arguments.

Check when reading offset << writing offset

Run `cleanLatencyFile.py MaterializeSort/latency_5ms.txt -1` to get the final `MaterializeSort/latency_5ms.csv`

Run `averageLatency.py MaterializeSort/latency_5ms.txt` to get average Latency of the experiment.

---

### Centralized MinTopK

Run the experiment using the 5 datasets that you can find in `dataset/`

Run `java -jar out/artifacts/minTopK_jar/kafka-stream-tutorial.jar "src/main/java/myapp/minTopK/minTopK.env"  topK dataset` where topK and dataset are integer values.

Run `RatingsDriver` with `centralized-mintopk-scored-rated-movies`, `INPUT_THROUGHPUT` and `DATASET` as arguments.

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

### Distributed Benchmark
Run 3 instances of `src/main/java/myapp/distributedMaterializeScoreSort/PhysicalWindow/PhysicalWindowDistributedMSS`.

Run `src/main/java/myapp/distributedMaterializeScoreSort/PhysicalWindow/PhysicalWindowCentralizedAggregatedSort`.

Run both files with `src/main/java/myapp/distributedMaterializeScoreSort/PhysicalWindow/physicalWindowDisMSS.env` as argument.

Run `RatingsDriver` with `pdmss-scored-rated-movies`, `INPUT_THROUGHPUT` and `DATASET` as arguments.

Check when reading offset << writing offset

Run `cleanLatencyFile.py DisMaterializeSort/latency_5ms.txt -1` to get the final `DisMaterializeSort/latency_5ms.csv`

Run `averageLatency.py DisMaterializeSort/latency_5ms.txt` to get average Latency of the experiment.

---

### Distributed MinTopK
Run 3 instances of `src/main/java/myapp/distributedMinTopK/DistributedMinTopK.java ENV_FILE TOPK DATASET #INSTANCE`.

Run `src/main/java/myapp/distributedMinTopK/CentralizedTopK.java ENV_FILE TOPK DATASET DisMinTopK`.

Run both files with `src/main/java/myapp/distributedMinTopK/disMinTopK.env` as first argument.

Run `RatingsDriver` with `dis-mintopk-scored-rated-movies`, `INPUT_THROUGHPUT` and `DATASET` as arguments.

Run `cleanEndTimeFile.py DisMinTopK/500Krecords_1200_300_topKK_end_time_5ms.txt topK` to get`DisMinTopK/500Krecords_1200_300_topKK_end_time_5ms.csv`

Run `cleanStartTimeFile.py DisMinTopK/dataset0/instance0_500Krecords_1200_300_2K_start_time_5ms.txt LocalWindowSize LocalWindowHoppingSize` for each instance's file

Run `distributedLatency.py instance0.csv instance1.csv instance2.csv endTime.csv` to get the final latency for each windows

---

### Distributed MinTopK+N

Run 3 instances of `src/main/java/myapp/distributedMinTopKN/DistributedMinTopKN.java ENV_FILE TOPK TOPN DATASET #INSTANCE`.

Run `src/main/java/myapp/distributedMinTopK/CentralizedTopK.java ENV_FILE TOPK DATASET DisMinTopKN`.

Run both files with `src/main/java/myapp/distributedMinTopKN/disMinTopKN.env` as first argument.

Run `RatingsDriver` with `dis-mintopkn-scored-rated-movies`, `INPUT_THROUGHPUT` and `DATASET` as arguments.

Run `cleanEndTimeFile.py DisMinTopKN/500Krecords_1200_300_topKK_end_time_5ms.txt topK` to get`DisMinTopKN/500Krecords_1200_300_topKK_end_time_5ms.csv`

Run `cleanStartTimeFile.py DisMinTopKN/dataset0/instance0_500Krecords_1200_300_2K_1N_start_time_5ms.txt LocalWindowSize LocalWindowHoppingSize` for each instance's file

Run `distributedLatency.py instance0.csv instance1.csv instance2.csv endTime.csv` to get the final latency for each windows

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
##### Box plot per dataset with same topK

Run `python3 plotBoxPlotPerDataset.py CentralizedMinTopK/dataset0/500Krecords_1200_300_2K_latency_5s.csv CentralizedMinTopK/dataset1/500Krecords_1200_300_2K_latency_5s.csv CentralizedMinTopK/dataset2/500Krecords_1200_300_2K_latency_5s.csv CentralizedMinTopK/dataset3/500Krecords_1200_300_2K_latency_5s.csv CentralizedMinTopK/dataset4/500Krecords_1200_300_2K_latency_5s.csv`

##### Box plot per topK

Run `python3 plotBoxPlot.py CentralizedMinTopK/dataset0/500Krecords_1200_300_2K_average.csv CentralizedMinTopK/dataset0/500Krecords_1200_300_10K_average.csv CentralizedMinTopK/dataset0/500Krecords_1200_300_50K_average.csv`
