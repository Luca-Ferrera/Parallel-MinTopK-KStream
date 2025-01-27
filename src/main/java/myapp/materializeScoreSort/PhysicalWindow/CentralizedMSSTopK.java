package myapp.materializeScoreSort.PhysicalWindow;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import myapp.ArrayListSerde;
import myapp.avro.ScoredMovie;
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

public class CentralizedMSSTopK {
    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("msstopk.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, envProps.getProperty("state.dir"));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }
    public Topology buildTopology(Properties envProps, String cleanDataStructure, int k, int dataset) {
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

        AtomicReference<Instant> start = new AtomicReference<>();
        AtomicReference<Instant> end = new AtomicReference<>();
        // TopKMovies
        builder.<String, ScoredMovie>stream(
                scoredMovieTopic
//                Materialized.<String, ScoredMovie, KeyValueStore<Bytes, byte[]>>as("scored-movies")
        )
                .map((key, value) ->{
                    start.set(Instant.now());
                    try(FileWriter fw = new FileWriter("measurements/CentralizedMSSTopK/top"+ k +"/dataset" + dataset + "/100Krecords_3600_300_" + k + "K_start_time.txt", true);
                        BufferedWriter bw = new BufferedWriter(fw);
                        PrintWriter out = new PrintWriter(bw))
                    {
                        out.println("Latency window " + key + " : " + start.get());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return new KeyValue<>(key,value);
                })
                .transform(new TransformerSupplier<String,ScoredMovie,KeyValue<Long , ScoredMovie>>() {
                    public Transformer get() {
                        return new MSSTopKTrasformer("windowed-movies-store",
                                "record-count-store", cleanDataStructure, k);
                    }
                }, "windowed-movies-store", "record-count-store")
                .map((key, value) -> {
                    end.set(Instant.now());
                    try(FileWriter fw = new FileWriter("measurements/CentralizedMSSTopK/dataset" + dataset + "/100Krecords_3600_300_" + k + "K_end_time.txt", true);
                        BufferedWriter bw = new BufferedWriter(fw);
                        PrintWriter out = new PrintWriter(bw))
                    {
                        out.println("Latency window " + key + " : " + end.get());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return new KeyValue<>(key,value);
                })
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
        int dataset = 0;
        int k = 0;
        if(args.length == 3){
            k = Integer.parseInt(args[1]);
            dataset = Integer.parseInt(args[2]);
        } else if(args.length == 4){
            k = Integer.parseInt(args[1]);
            dataset = Integer.parseInt(args[2]);
            cleanDataStructure = args[3];
        }
        if(args.length == 2){
            cleanDataStructure = args[1];
        }
        CentralizedMSSTopK msst= new CentralizedMSSTopK();
        Properties envProps = msst.loadEnvProperties(args[0]);
        Properties streamProps = msst.buildStreamsProperties(envProps);
        Topology topology = msst.buildTopology(envProps, cleanDataStructure, k, dataset);
        System.out.println(topology.describe());

        msst.createTopics(envProps);

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


