package myapp.exercises;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import myapp.MovieRatingJoiner;
import myapp.avro.RatedMovie;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

import myapp.avro.Movie;
import myapp.avro.Rating;
import org.apache.kafka.streams.kstream.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class JoinStreamToTable {
    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
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
        final String summedRatingTopic = envProps.getProperty("summed.rated.movies.topic.name");
        final String averagedRatingTopic = envProps.getProperty("averaged.rated.movies.topic.name");
        final MovieRatingJoiner joiner = new MovieRatingJoiner();

        KStream<String, Movie> movieStream = builder.<String, Movie>stream(movieTopic)
                .map((key, movie)-> new KeyValue<String,Movie>(movie.getId().toString(), movie));
        movieStream.to(rekeyedMovieTopic);

        KTable<String, Movie> movies = builder.table(rekeyedMovieTopic);

        KStream<String, Rating> ratings = builder.<String, Rating>stream(ratingTopic)
                .map((key, rating) -> new KeyValue<String, Rating>(rating.getId().toString(), rating));

        KStream<String, RatedMovie> ratedMovie = ratings.join(movies, joiner);
        ratedMovie.to(ratedMovieTopic, Produced.with(Serdes.String(), ratedMovieAvroSerde(envProps)));

        // Sum Ratings
        ratedMovie.groupBy((key, movie) -> movie.getTitle().toString())
                .reduce((aggValue, newValue) -> {
                    aggValue.setRating(aggValue.getRating() + newValue.getRating());
                    return aggValue;
                })
                .toStream()
                .to(summedRatingTopic, Produced.with(Serdes.String(),ratedMovieAvroSerde(envProps)));

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

    private GenericAvroSerde genericAvroSerde(Properties envProps) {
        GenericAvroSerde genericAvroSerde = new GenericAvroSerde();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        genericAvroSerde.configure(serdeConfig, false);
        return genericAvroSerde;
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
                envProps.getProperty("summed.rated.movies.topic.name"),
                Integer.parseInt(envProps.getProperty("summed.rated.movies.topic.partitions")),
                Short.parseShort(envProps.getProperty("summed.rated.movies.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("averaged.rated.movies.topic.name"),
                Integer.parseInt(envProps.getProperty("averaged.rated.movies.topic.partitions")),
                Short.parseShort(envProps.getProperty("averaged.rated.movies.topic.replication.factor"))));

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

        JoinStreamToTable ts = new JoinStreamToTable();
        Properties envProps = ts.loadEnvProperties(args[0]);
        Properties streamProps = ts.buildStreamsProperties(envProps);
        Topology topology = ts.buildTopology(envProps);
        System.out.println(topology.describe());

        ts.createTopics(envProps);

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
