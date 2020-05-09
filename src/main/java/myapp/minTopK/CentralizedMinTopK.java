package myapp.minTopK;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import myapp.MovieAverageJoiner;
import myapp.MovieRatingJoiner;
import myapp.avro.*;
import myapp.transormers.MinTopKTransformer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class CentralizedMinTopK {
    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("minTopK.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }
    public Topology buildTopology(Properties envProps, String cleanDataStructure) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String movieTopic = envProps.getProperty("movie.topic.name");
        final String ratingTopic = envProps.getProperty("rating.topic.name");
        final String rekeyedMovieTopic = envProps.getProperty("rekeyed.movie.topic.name");
        final String ratedMovieTopic = envProps.getProperty("rated.movies.topic.name");
        final String scoredMovieTopic = envProps.getProperty("scored.movies.topic.name");
        final String averagedRatedMovieTopic = envProps.getProperty("averaged.rated.movies.topic.name");
        final String minTopKRatedMovie = envProps.getProperty("mintopk.movies.topic.name");
        final MovieRatingJoiner joiner = new MovieRatingJoiner();
        final MovieAverageJoiner averageJoiner = new MovieAverageJoiner();

        // create intermediate-topK-movies store
        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("super-topk-list-store"),
                Serdes.Integer(),
                minTopKEntryAvroSerde(envProps));
        // register store
        builder.addStateStore(storeBuilder);

        // create windows store
        StoreBuilder windowsStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("windows-store"),
                Serdes.Long(),
                physicalWindowAvroSerde(envProps));
        // register store
        builder.addStateStore(windowsStoreBuilder);

//        KStream<String, Movie> movieStream = builder.<String, Movie>stream(movieTopic)
//                .map((key, movie)-> new KeyValue<String,Movie>(movie.getId().toString(), movie));
//        movieStream.to(rekeyedMovieTopic);
//
//        KTable<String, Movie> movies = builder.table(rekeyedMovieTopic);
//
//        KStream<String, Rating> ratings = builder.<String, Rating>stream(ratingTopic)
//                .map((key, rating) -> new KeyValue<String, Rating>(rating.getId().toString(), rating));
//
//        KStream<String, RatedMovie> ratedMovie = ratings.join(movies, joiner);
//        ratedMovie.to(ratedMovieTopic, Produced.with(Serdes.String(), ratedMovieAvroSerde(envProps)));
//
//        KGroupedStream<String ,Double> ratingsById = ratings
//                .map((key, value)-> new KeyValue<String, Double>(key, value.getRating()))
//                .groupByKey(Grouped.valueSerde(Serdes.Double()));
//        KTable<String, Long> ratingCount = ratingsById.count();
//        KTable<String, Double> ratingSum = ratingsById.reduce(Double::sum);
//        KTable<String, Double> ratingAverage = ratingSum
//                .join(ratingCount,
//                        (sum, count) -> sum/count.doubleValue(),
//                        Materialized.<String, Double, KeyValueStore<org.apache.kafka.common.utils.Bytes,byte[]>>as("average-ratings")
//                                .withValueSerde(Serdes.Double()));
//
//        ratingAverage.toStream().to(averagedRatedMovieTopic, Produced.with(Serdes.String(), Serdes.Double()));
//
//        //ScoredMovie
//        KStream<String, AverageMovie> averageMovie  = movies.join(ratingAverage, averageJoiner).toStream();
//
//        averageMovie.mapValues((key, movie)->{
//            double score = movie.getAverage()/10 * 0.8 + movie.getReleaseYear()/2020 * 0.2;
//            return new ScoredMovie(movie, score);
//        })
//                .to(scoredMovieTopic, Produced.with(Serdes.String(), scoredMovieAvroSerde(envProps)));
        AtomicReference<Instant> start = new AtomicReference<>();
        AtomicReference<Instant> end = new AtomicReference<>();
        // TopKMovies
        builder.<String,ScoredMovie>stream(scoredMovieTopic)
                .map((key, value) ->{
                    start.set(Instant.now());
                    return new KeyValue<>(key,value);
                })
                .transform(new TransformerSupplier<String,ScoredMovie,KeyValue<Long , MinTopKEntry>>() {
                    public Transformer get() {
                        return new MinTopKTransformer(2, cleanDataStructure);
                    }
                }, "windows-store", "super-topk-list-store")
                .map((key, value) ->{
                    end.set(Instant.now());
                    try(FileWriter fw = new FileWriter("latency_50ms.txt", true);
                        BufferedWriter bw = new BufferedWriter(fw);
                        PrintWriter out = new PrintWriter(bw))
                    {
                        out.println("Latency window " + key + ": " + Duration.between(start.get(), end.get()).toNanos());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println("key: " + key + " value: " + value);
                    return new KeyValue<>(key,value);
                })
                .to(minTopKRatedMovie, Produced.with(Serdes.Long(),minTopKEntryAvroSerde(envProps)));

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
    private SpecificAvroSerde<PhysicalWindow> physicalWindowAvroSerde(Properties envProps) {
        SpecificAvroSerde<PhysicalWindow> physicalWindowAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        physicalWindowAvroSerde.configure(serdeConfig, false);
        return physicalWindowAvroSerde;
    }
    private SpecificAvroSerde<MinTopKEntry> minTopKEntryAvroSerde(Properties envProps) {
        SpecificAvroSerde<MinTopKEntry> minTopKEntryAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        minTopKEntryAvroSerde.configure(serdeConfig, false);
        return minTopKEntryAvroSerde;
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
                envProps.getProperty("mintopk.movies.topic.name"),
                Integer.parseInt(envProps.getProperty("mintopk.movies.topic.partitions")),
                Short.parseShort(envProps.getProperty("mintopk.movies.topic.replication.factor"))));

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

        String cleanDataStructure = "";
        if(args.length == 2)
            cleanDataStructure = args[1];

        CentralizedMinTopK minTopK = new CentralizedMinTopK();
        Properties envProps = minTopK.loadEnvProperties(args[0]);
        Properties streamProps = minTopK.buildStreamsProperties(envProps);
        Topology topology = minTopK.buildTopology(envProps, cleanDataStructure);
        System.out.println(topology.describe());

        minTopK.createTopics(envProps);

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
