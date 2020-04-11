package myapp;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import myapp.avro.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class MaterializeScoreSort {
    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("mss.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }
    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String movieTopic = envProps.getProperty("movie.topic.name");
        final String ratingTopic = envProps.getProperty("rating.topic.name");
        final String rekeyedMovieTopic = envProps.getProperty("rekeyed.movie.topic.name");
        final String ratedMovieTopic = envProps.getProperty("rated.movies.topic.name");
        final String scoredMovieTopic = envProps.getProperty("scored.movies.topic.name");
        final String averagedRatedMovieTopic = envProps.getProperty("averaged.rated.movies.topic.name");
        final String sortedScoredMovieTopic = envProps.getProperty("sorted.rated.movies.topic.name");
        final MovieRatingJoiner joiner = new MovieRatingJoiner();
        final MovieAverageJoiner averageJoiner = new MovieAverageJoiner();

        KStream<String, Movie> movieStream = builder.<String, Movie>stream(movieTopic)
                .map((key, movie)-> new KeyValue<String,Movie>(movie.getId().toString(), movie));
        movieStream.to(rekeyedMovieTopic);

        KTable<String, Movie> movies = builder.table(rekeyedMovieTopic);

        KStream<String, Rating> ratings = builder.<String, Rating>stream(ratingTopic)
                .map((key, rating) -> new KeyValue<String, Rating>(rating.getId().toString(), rating));

        KStream<String, RatedMovie> ratedMovie = ratings.join(movies, joiner);
        ratedMovie.to(ratedMovieTopic, Produced.with(Serdes.String(), ratedMovieAvroSerde(envProps)));

        KGroupedStream<String ,Double> ratingsById = ratings
                .map((key, value)-> new KeyValue<String, Double>(key, value.getRating()))
                .groupByKey(Grouped.valueSerde(Serdes.Double()));
        KTable<String, Long> ratingCount = ratingsById.count();
        KTable<String, Double> ratingSum = ratingsById.reduce((v1, v2) -> v1 + v2);
        KTable<String, Double> ratingAverage = ratingSum
                .join(ratingCount,
                        (sum, count) -> sum/count.doubleValue(),
                        Materialized.<String, Double, KeyValueStore<org.apache.kafka.common.utils.Bytes,byte[]>>as("average-ratings")
                                .withValueSerde(Serdes.Double()));

        ratingAverage.toStream().to(averagedRatedMovieTopic, Produced.with(Serdes.String(), Serdes.Double()));

        //ScoredMovie
        KStream<String, AverageMovie> averageMovie  = movies.join(ratingAverage, averageJoiner).toStream();

        averageMovie.mapValues((key, movie)->{
            double score = movie.getAverage()/10 * 0.8 + movie.getReleaseYear()/2020 * 0.2;
            return new ScoredMovie(movie, score);
        })
                .to(scoredMovieTopic, Produced.with(Serdes.String(), scoredMovieAvroSerde(envProps)));

        // SortedMovie
        KStream<String,ScoredMovie> scoredStream = builder.table(
                scoredMovieTopic,
                Consumed.with(Serdes.String(),scoredMovieAvroSerde(envProps)),
                Materialized.<String, ScoredMovie, KeyValueStore<Bytes, byte[]>>as("scored-movies")
        ).toStream();

        return builder.build();
    }

    private SpecificAvroSerde<RatedMovie> ratedMovieAvroSerde(Properties envProps) {
        SpecificAvroSerde<RatedMovie> movieAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        movieAvroSerde.configure(serdeConfig, false);
        return movieAvroSerde;
    }

    private SpecificAvroSerde<ScoredMovie> scoredMovieAvroSerde(Properties envProps) {
        SpecificAvroSerde<ScoredMovie> movieAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        movieAvroSerde.configure(serdeConfig, false);
        return movieAvroSerde;
    }


    public void createTopics(Properties envProps) {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", envProps.getProperty("bootstrap.servers"));
        AdminClient client = AdminClient.create(config);

        List<NewTopic> topics = new ArrayList<>();

        topics.add(new NewTopic(
                envProps.getProperty("movie.topic.name"),
                Integer.parseInt(envProps.getProperty("movie.topic.partitions")),
                Short.parseShort(envProps.getProperty("movie.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("rekeyed.movie.topic.name"),
                Integer.parseInt(envProps.getProperty("rekeyed.movie.topic.partitions")),
                Short.parseShort(envProps.getProperty("rekeyed.movie.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("rating.topic.name"),
                Integer.parseInt(envProps.getProperty("rating.topic.partitions")),
                Short.parseShort(envProps.getProperty("rating.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("rated.movies.topic.name"),
                Integer.parseInt(envProps.getProperty("rated.movies.topic.partitions")),
                Short.parseShort(envProps.getProperty("rated.movies.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("scored.movies.topic.name"),
                Integer.parseInt(envProps.getProperty("scored.movies.topic.partitions")),
                Short.parseShort(envProps.getProperty("scored.movies.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("averaged.rated.movies.topic.name"),
                Integer.parseInt(envProps.getProperty("averaged.rated.movies.topic.partitions")),
                Short.parseShort(envProps.getProperty("averaged.rated.movies.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("sorted.rated.movies.topic.name"),
                Integer.parseInt(envProps.getProperty("sorted.rated.movies.topic.partitions")),
                Short.parseShort(envProps.getProperty("sorted.rated.movies.topic.replication.factor"))));

        client.createTopics(topics);
        client.close();
    }

    public Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();
        FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();

        return envProps;
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        MaterializeScoreSort mss = new MaterializeScoreSort();
        Properties envProps = mss.loadEnvProperties(args[0]);
        Properties streamProps = mss.buildStreamsProperties(envProps);
        Topology topology = mss.buildTopology(envProps);
        System.out.println(topology.describe());

        mss.createTopics(envProps);

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);

        CompletableFuture<KafkaStreams.State> stateFuture = new CompletableFuture<>();
        streams.setStateListener((newState, oldState) -> {
            if(stateFuture.isDone()) {
                return;
            }

            if(newState == KafkaStreams.State.RUNNING || newState == KafkaStreams.State.ERROR) {
                stateFuture.complete(newState);
            }
        });

        streams.cleanUp();
        streams.start();
        try {
            KafkaStreams.State finalState = stateFuture.get();
            if(finalState == KafkaStreams.State.RUNNING) {
                ReadOnlyKeyValueStore<String, ScoredMovie> localStore = streams.store("scored-movies", QueryableStoreTypes.<String, ScoredMovie>keyValueStore());
                ArrayList<KeyValue<String, ScoredMovie>> scoreList = new ArrayList<>();
                localStore.all().forEachRemaining((elem)-> {
                    KeyValue<String,ScoredMovie> entry = new KeyValue<String, ScoredMovie>(elem.key, elem.value);
                    scoreList.add(entry);
                });
                Comparator<KeyValue<String, ScoredMovie>> compareByScore = Comparator.comparingDouble((KeyValue<String, ScoredMovie> o) -> o.value.getScore());
                Collections.sort(scoreList, compareByScore.reversed()
                );
                scoreList.forEach(elem -> System.out.println("key " + elem.key + " value " + elem.value));
            }
        } catch (InterruptedException ex) {
            System.out.println(ex);
        } catch(ExecutionException ex) {
            System.out.println(ex);
        }


        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            try{
                streams.close();
            } catch (final Exception e){
                System.out.println(e);
            }
        }));
    }
}
