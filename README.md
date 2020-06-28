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

Run `RatingsDriver` with `scored-rated-movies`, `INPUT_THROUGHPUT` and `DATASET_NUMBER` as arguments.

Check when reading offset << writing offset

Run `cleanLatencyFile.py MaterializeSort/latency_5ms.txt -1` to get the final `MaterializeSort/latency_5ms.csv`

Run `averageLatency.py MaterializeSort/latency_5ms.txt` to get average Latency of the experiment.

---

### Centralized MinTopK

Run the experiment using the 5 datasets that you can find in `dataset/`

Run `java -jar out/artifacts/minTopK_jar/kafka-stream-tutorial.jar "src/main/java/myapp/minTopK/minTopK.env"  topK dataset` where topK and dataset are integer values.

Run `RatingsDriver` with `centralized-mintopk-scored-rated-movies`, `INPUT_THROUGHPUT` and `DATASET_NUMBER` as arguments.

Run `cleanLatencyFile.py CentralizedMinTopK/topKK_latency_5ms.txt topK` to get the final `CentralizedMinTopK/topKK_latency_5ms.csv`

Run `averageLatency.py CentralizedMinTopK/topKK_latency_5ms.txt` to get average Latency of the experiment.

---

### Distributed Benchmark
Run 3 instances of `src/main/java/myapp/distributedMaterializeScoreSort/PhysicalWindow/PhysicalWindowDistributedMSS`.

Run `src/main/java/myapp/distributedMaterializeScoreSort/PhysicalWindow/PhysicalWindowCentralizedAggregatedSort`.

Run both files with `src/main/java/myapp/distributedMaterializeScoreSort/PhysicalWindow/physicalWindowDisMSS.env` as argument.

Run `RatingsDriver` with `pdmss-scored-rated-movies`, `INPUT_THROUGHPUT` and `DATASET_NUMBER` as arguments.

Check when reading offset << writing offset

Run `cleanLatencyFile.py DisMaterializeSort/latency_5ms.txt -1` to get the final `DisMaterializeSort/latency_5ms.csv`

Run `averageLatency.py DisMaterializeSort/latency_5ms.txt` to get average Latency of the experiment.

### Distributed MinTopK
Run 3 instances of `src/main/java/myapp/distributedMinTopK/DistributedMinTopK.java`.

Run `src/main/java/myapp/distributedMinTopK/CentralizedTopK.java`.

Run both files with `src/main/java/myapp/distributedMinTopK/disMinTopK.env` as argument.

Run `RatingsDriver` with `dis-mintopk-scored-rated-movies`, `INPUT_THROUGHPUT` and `DATASET_NUMBER` as arguments.

Run `cleanLatencyFile.py DisMinTopK/topKK_latency_5ms.txt topK` to get the final `DisMinTopK/topKK_latency_5ms.csv`

Run `averageLatency.py DisMinTopK/topKK_latency_5ms.txt` to get average Latency of the experiment.

