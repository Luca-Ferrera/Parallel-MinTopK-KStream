package myapp.minTopK;

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
    public Topology buildTopology(Properties envProps, String cleanDataStructure, int k, int dataset) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String scoredMovieTopic = envProps.getProperty("scored.movies.topic.name");
        final String minTopKRatedMovie = envProps.getProperty("mintopk.movies.topic.name");

        // create super-topK-list store
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
                    try(FileWriter fw = new FileWriter("measurements/CentralizedMinTopK/dataset" + dataset + "/100Krecords_3600_300_" + k + "K_start_time.txt", true);
                        BufferedWriter bw = new BufferedWriter(fw);
                        PrintWriter out = new PrintWriter(bw))
                    {
                        out.println("Latency window " + key + " : " + start.get());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return new KeyValue<>(key,value);
                })
                .transform(new TransformerSupplier<String,ScoredMovie,KeyValue<Long , MinTopKEntry>>() {
                    public Transformer get() {
                        return new MinTopKTransformer(k, cleanDataStructure);
                    }
                }, "windows-store", "super-topk-list-store")
                .map((key, value) ->{
                    end.set(Instant.now());
                    try(FileWriter fw = new FileWriter("measurements/CentralizedMinTopK/dataset" + dataset + "/100Krecords_3600_300_" + k + "K_end_time.txt", true);
                        BufferedWriter bw = new BufferedWriter(fw);
                        PrintWriter out = new PrintWriter(bw))
                    {
                        out.println("Latency window " + key + " : " + end.get());
                    } catch (IOException e) {
                       e.printStackTrace();
                    }
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
        int k = 0;
        int dataset = 0;
        if(args.length == 3){
            k = Integer.parseInt(args[1]);
            dataset = Integer.parseInt(args[2]);
        } else if(args.length == 4){
            cleanDataStructure = args[3];
            dataset = Integer.parseInt(args[2]);
            k = Integer.parseInt(args[1]);
        }

        CentralizedMinTopK minTopK = new CentralizedMinTopK();
        Properties envProps = minTopK.loadEnvProperties(args[0]);
        Properties streamProps = minTopK.buildStreamsProperties(envProps);
        Topology topology = minTopK.buildTopology(envProps, cleanDataStructure, k, dataset);
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
