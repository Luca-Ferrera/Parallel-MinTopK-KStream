package myapp.minTopKN;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import myapp.avro.*;
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

public class CentralizedMinTopKN {
    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("minTopKN.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }
    public Topology buildTopology(Properties envProps, String cleanDataStructure, int k, int n, int dataset) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String scoredMovieTopic = envProps.getProperty("scored.movies.topic.name");
        final String minTopKRatedMovie = envProps.getProperty("mintopkn.movies.topic.name");
        final String updatesTopic = envProps.getProperty("mintopkn.updates.topic.name");

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
                        return new MinTopKNTransformer(k, n, cleanDataStructure, updatesTopic);
                    }
                }, "windows-store", "super-topk-list-store")
                .map((key, value) ->{
                    end.set(Instant.now());
                    try(FileWriter fw = new FileWriter("CentralizedMinTopKN/dataset" + dataset + "/500Krecords_1200_300_" + k + "K_" + n + "N_latency_5s.txt", true);
                        BufferedWriter bw = new BufferedWriter(fw);
                        PrintWriter out = new PrintWriter(bw))
                    {
                        out.println("Latency window " + key + " : " + Duration.between(start.get(), end.get()).toNanos());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    if(key % 500 == 0)
                        System.out.println("key: " + key + " value: " + value);
                    return new KeyValue<>(key,value);
                })
                .to(minTopKRatedMovie, Produced.with(Serdes.Long(),minTopKEntryAvroSerde(envProps)));

        return builder.build();
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
                envProps.getProperty("scored.movies.topic.name"),
                Integer.parseInt(envProps.getProperty("scored.movies.topic.partitions")),
                Short.parseShort(envProps.getProperty("scored.movies.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("mintopkn.movies.topic.name"),
                Integer.parseInt(envProps.getProperty("mintopkn.movies.topic.partitions")),
                Short.parseShort(envProps.getProperty("mintopkn.movies.topic.replication.factor"))));

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
        int k = 0, n = 0, dataset = 0;
        if(args.length == 4){
            k = Integer.parseInt(args[1]);
            n = Integer.parseInt(args[2]);
            dataset = Integer.parseInt(args[3]);
        } else if(args.length == 5){
            cleanDataStructure = args[4];
            dataset = Integer.parseInt(args[3]);
            n = Integer.parseInt(args[2]);
            k = Integer.parseInt(args[1]);
        }
        CentralizedMinTopKN minTopKN = new CentralizedMinTopKN();
        Properties envProps = minTopKN.loadEnvProperties(args[0]);
        Properties streamProps = minTopKN.buildStreamsProperties(envProps);
        Topology topology = minTopKN.buildTopology(envProps, cleanDataStructure, k, n, dataset);
        System.out.println(topology.describe());

        minTopKN.createTopics(envProps);

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

