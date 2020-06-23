package myapp.distributedMaterializeScoreSort.PhysicalWindow;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import myapp.ArrayListSerde;
import myapp.avro.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class PhysicalWindowDistributedMSS {
    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("disMSS.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }
    public Topology buildTopology(Properties envProps, String cleanDataStructure, int k) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String scoredMovieTopic = envProps.getProperty("scored.movies.topic.name");
        final String sortedTopKMovieTopic = envProps.getProperty("sorted.movies.topic.name");


        StoreBuilder countStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("record-count-store"),
                Serdes.Integer(),
                Serdes.Integer());
        // register store
        builder.addStateStore(countStoreBuilder);

        // create windowed-topk store
        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("windowed-movies-store"),
                Serdes.Long(),
                new ArrayListSerde(scoredMovieAvroSerde(envProps)));
        // register store
        builder.addStateStore(storeBuilder);

        // TopKMovies
        builder.<String,ScoredMovie>stream(
                scoredMovieTopic
//                Materialized.<String, ScoredMovie, KeyValueStore<Bytes, byte[]>>as("scored-movies")
        )
//                .toStream()
                .map((key, value) ->{
                    return new KeyValue<>(key,value);
                })
                .transform(new TransformerSupplier<String,ScoredMovie,KeyValue<Long , ScoredMovie>>() {
                    public Transformer get() {
                        return new DistributedTopKTransformer( "windowed-movies-store",
                                 "record-count-store", k, cleanDataStructure );
                    }
                }, "windowed-movies-store", "record-count-store")
                .to(sortedTopKMovieTopic, Produced.with(Serdes.Long(), scoredMovieAvroSerde(envProps)));

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
                envProps.getProperty("sorted.movies.topic.name"),
                Integer.parseInt(envProps.getProperty("sorted.movies.topic.partitions")),
                Short.parseShort(envProps.getProperty("sorted.movies.topic.replication.factor"))));

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
        int k = 0;
        if(args.length == 2){
            k = Integer.parseInt(args[1]);
        }else if(args.length == 3){
            cleanDataStructure = args[2];
            k = Integer.parseInt(args[1]);
        }
        System.out.println(cleanDataStructure);
        PhysicalWindowDistributedMSS dmss = new PhysicalWindowDistributedMSS();
        Properties envProps = dmss.loadEnvProperties(args[0]);
        Properties streamProps = dmss.buildStreamsProperties(envProps);
        Topology topology = dmss.buildTopology(envProps, cleanDataStructure, k);
        System.out.println(topology.describe());

        dmss.createTopics(envProps);

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

