package myapp.distributedMinTopK;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import myapp.ArrayListSerde;
import myapp.avro.MinTopKEntry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class CentralizedTopK {
    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("minTopK.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }
    public Topology buildTopology(Properties envProps, String cleanDataStructure, int k, int dataset) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String minTopKRatedMovie = envProps.getProperty("mintopk.movies.topic.name");
        final String windowedTopKTopic = envProps.getProperty("windowed.mintopk.topic.name");

        // create windowed-topk store
        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("windowed-topk-store"),
                Serdes.Long(),
                new ArrayListSerde(minTopKEntryAvroSerde(envProps)));
        // register store
        builder.addStateStore(storeBuilder);



        AtomicReference<Instant> end = new AtomicReference<>();
        // TopKMovies
        builder.<Long,MinTopKEntry>stream(minTopKRatedMovie)
                .transform(new TransformerSupplier<Long,MinTopKEntry,KeyValue<Long , MinTopKEntry>>() {
                    public Transformer get() {
                        return new CentralizedTopKTransformer(k, cleanDataStructure);
                    }
                }, "windowed-topk-store")
                .map((key, value) ->{
                    end.set(Instant.now());
                    try(FileWriter fw = new FileWriter("DisMinTopK/dataset" + dataset + "/500Krecords_1200_300_" + k + "K_end_time_5ms.txt", true);
                        BufferedWriter bw = new BufferedWriter(fw);
                        PrintWriter out = new PrintWriter(bw))
                    {
                        out.println("Latency window " + key + " : " + end.get());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return new KeyValue<>(key,value);
                })
                .to(windowedTopKTopic, Produced.with(Serdes.Long(),minTopKEntryAvroSerde(envProps)));

        return builder.build();
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
        Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put("retention.ms", envProps.getProperty("retention.ms"));
        topicConfig.put("retention.bytes", envProps.getProperty("retention.bytes"));

        List<NewTopic> topics = new ArrayList<>();


        topics.add(new NewTopic(
                envProps.getProperty("mintopk.movies.topic.name"),
                Integer.parseInt(envProps.getProperty("mintopk.movies.topic.partitions")),
                Short.parseShort(envProps.getProperty("mintopk.movies.topic.replication.factor"))).configs(topicConfig));

        topics.add(new NewTopic(
                envProps.getProperty("windowed.mintopk.topic.name"),
                Integer.parseInt(envProps.getProperty("windowed.mintopk.topic.partitions")),
                Short.parseShort(envProps.getProperty("windowed.mintopk.topic.replication.factor"))).configs(topicConfig));

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

        CentralizedTopK centralizedTopK = new CentralizedTopK();
        Properties envProps = centralizedTopK.loadEnvProperties(args[0]);
        Properties streamProps = centralizedTopK.buildStreamsProperties(envProps);
        Topology topology = centralizedTopK.buildTopology(envProps, cleanDataStructure, k, dataset);
        System.out.println(topology.describe());

        centralizedTopK.createTopics(envProps);

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

