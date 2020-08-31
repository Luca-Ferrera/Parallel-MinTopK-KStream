# Centralized MinTopK+N algorithm
## How to try it

Run `CentralizedMinTopKN` with `"minTopK.env" K N dataset` as argument.
Where K is the value of the topK parameter, N is the value of the updates in the distributed DB and dataset is the id of the dataset.

Run `docker exec -i schema-registry /usr/bin/kafka-avro-console-producer --topic movie-updates --broker-list broker:9092 --property value.schema="$(< src/main/avro/scored-movie.avsc)"`
in order to simulate updates in a distributed DB.
Example of records to be sent can be found in `score-movies-test.txt`.
## Clean data structures

Run `CentralizedMinTopKN` with `"minTopK.env" K N dataset m"clean"` as arguments.

## Generate input data

Run `RatingsDriver` with `centralized-mintopkn-scored-rated-movies`, `INPUT_THROUGHPUT` and `DATASET_NUMBER` as arguments, it generates records like  this one:
```
{"id": 294, "title": "Die Hard", "release_year": 1988, "rating": 4.496183638158378, "score": 0.5565263742209872}
```

## Output
 
output topic: `centralized-mintopkn-movies`

output format: 
```
key = windowID (Long)
value = MinTopKEntry
```
