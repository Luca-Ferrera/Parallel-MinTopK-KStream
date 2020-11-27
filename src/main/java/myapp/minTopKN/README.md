# Centralized MinTopK+N algorithm
## How to try it

Run `CentralizedMinTopKN` with `"minTopK.env" K N dataset` as argument.
Where K is the value of the topK parameter, N is the value of the updates in the distributed DB and dataset is the id of the dataset.

### Updates without Kafka Connect

Run `docker exec -i schema-registry /usr/bin/kafka-avro-console-producer --topic movie-updates --broker-list broker:9092 --property value.schema="$(< src/main/avro/scored-movie.avsc)"`
in order to simulate updates in a distributed DB.
Example of records to be sent can be found in `score-movies-test.txt`.

### Updates with Kafka Connect
Generate the Connector running 
```
curl -X POST \
        -H "Content-Type: application/json" \
        --data '{ "name": "mintopkn-jdbc-source", "config": {
          "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
          "tasks.max": 3,
          "connection.url": "jdbc:mysql://mysql:3306/mintopkn?user=luca&password=passwd",
          "mode": "timestamp+incrementing",
          "incrementing.column.name": "id",
          "timestamp.column.name": "modified",
          "topic.prefix": "mintopkn-jdbc-",
          "poll.interval.ms": 1000,
          "transforms" : "AddNamespace",
          "transforms.AddNamespace.type" : "org.apache.kafka.connect.transforms.SetSchemaMetadata$Value",
          "transforms.AddNamespace.schema.name" : "myapp.avro.MovieIncome"} }' \
        http://$CONNECT_HOST:28083/connectors
```

Run `python updateDB.py` to perform random updates of the movie's income in the MySQL DB.

Updates are stored in `mintopkn-jdbc-updates` topic, which is read by a specific `KafkaConsumer` inside the algorithm.

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
