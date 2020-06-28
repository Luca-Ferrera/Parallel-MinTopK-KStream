package myapp;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import static java.nio.charset.StandardCharsets.UTF_8;
import myapp.avro.ScoredMovie;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;

import java.io.*;
import java.util.*;

public class RatingsDriver {
    public static void main(final String [] args) throws Exception {
        final String bootstrapServers = "localhost:29092";
        final String schemaRegistryUrl = "http://localhost:8081";
        final String INPUT_TOPIC = args.length > 0 ? args[0] : "centralized-mintopk-scored-rated-movies";
        final Long INPUT_THROUGHPUT = args.length > 1 ? Long.parseLong(args[1]) : 5L;
        final int dataset = args.length > 2 ? Integer.parseInt(args[2]) : 0;
        System.out.println("Connecting to Kafka cluster via bootstrap servers " + bootstrapServers);
        System.out.println("Connecting to Confluent schema registry at " + schemaRegistryUrl);

        // Read comma-delimited file of songs into Array
        final List<ScoredMovie> scoredMovies = new ArrayList<>();
        File initialFile = new File("dataset/score-movies" + dataset +".txt");
        InputStream targetStream = new FileInputStream(initialFile);
        final InputStreamReader streamReader = new InputStreamReader(targetStream, UTF_8);
        try (final BufferedReader br = new BufferedReader(streamReader)) {
            String line;
            while ((line = br.readLine()) != null) {
                final ScoredMovie newScoredMovie = new ObjectMapper().readValue(line, ScoredMovie.class);
                scoredMovies.add(newScoredMovie);
            }
        }

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class);

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        final SpecificAvroSerializer<ScoredMovie> scoredMovieSerializer = new SpecificAvroSerializer<>();
        scoredMovieSerializer.configure(serdeConfig, false);

        final KafkaProducer<String, ScoredMovie> scoredMovieProducer = new KafkaProducer<>(props,
                Serdes.String().serializer(),
                scoredMovieSerializer);

        //sending movies every INPUT_THROUGHPUT ms
        final Integer[] i = {0};
        scoredMovies.forEach(movie -> {
            scoredMovieProducer.send(new ProducerRecord<String, ScoredMovie>(INPUT_TOPIC, null, movie),
            new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if(e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.println("The offset of the record " + i[0] + " we just sent is: " + metadata.offset());
                    }
                }
            });
            try {
                Thread.sleep(INPUT_THROUGHPUT);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            i[0]++;
        });
    }
}
