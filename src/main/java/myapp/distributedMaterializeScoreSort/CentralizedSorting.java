package myapp.distributedMaterializeScoreSort;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import myapp.MyTransformer;
import myapp.avro.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class CentralizedSorting {
    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("cs.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }

    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String scoredMovieTopic = envProps.getProperty("scored.movies.topic.name");
        final String sortedScoredMovieTopic = envProps.getProperty("sorted.rated.movies.topic.name");

        // SortedMovie
        builder.table(
                scoredMovieTopic,
                Consumed.with(Serdes.String(), scoredMovieAvroSerde(envProps)),
                Materialized.<String, ScoredMovie, KeyValueStore<Bytes, byte[]>>as("scored-movies")
        )
                .toStream()
                .transform(new TransformerSupplier<String,ScoredMovie,KeyValue<String , ScoredMovie>>() {
                    public Transformer get() {
                        return new MyTransformer();
                    }
                }, "scored-movies")
                .to(sortedScoredMovieTopic, Produced.with(Serdes.String(), scoredMovieAvroSerde(envProps)));

        return builder.build();
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
                envProps.getProperty("scored.movies.topic.name"),
                Integer.parseInt(envProps.getProperty("scored.movies.topic.partitions")),
                Short.parseShort(envProps.getProperty("scored.movies.topic.replication.factor"))));

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

        CentralizedSorting centralizedSorting = new CentralizedSorting();
        Properties envProps = centralizedSorting.loadEnvProperties(args[0]);
        Properties streamProps = centralizedSorting.buildStreamsProperties(envProps);
        Topology topology = centralizedSorting.buildTopology(envProps);
        System.out.println(topology.describe());

        centralizedSorting.createTopics(envProps);

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

