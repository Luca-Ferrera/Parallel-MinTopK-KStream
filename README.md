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

Run the application to generate the topics (just do it the first time)

Run `docker exec -i schema-registry /usr/bin/kafka-avro-console-producer --topic movies --broker-list broker:9092 --property value.schema="$(< src/main/avro/movie.avsc)"` to open a console to input movies data

Run `docker exec -i schema-registry /usr/bin/kafka-avro-console-producer --topic ratings --broker-list broker:9092 --property value.schema="$(< src/main/avro/rating.avsc)"` to open a console to input ratings data

Run `docker exec -it schema-registry /usr/bin/kafka-avro-console-consumer --topic TOPIC_NAME --bootstrap-server broker:9092 --from-beginning` to read data from `TOPIC_NAME` topic

Start the application again!