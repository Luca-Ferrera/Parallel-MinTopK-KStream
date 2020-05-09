package myapp;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import static java.nio.charset.StandardCharsets.UTF_8;
import myapp.avro.ScoredMovie;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.io.*;
import java.util.*;

public class RatingsDriver {
    static String INPUT_TOPIC = "mintopk-scored-rated-movies";
    static Long INPUT_THROUGHPUT = 50L;
    public static void main(final String [] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:29092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        System.out.println("Connecting to Kafka cluster via bootstrap servers " + bootstrapServers);
        System.out.println("Connecting to Confluent schema registry at " + schemaRegistryUrl);

        // Read comma-delimited file of songs into Array
        final List<ScoredMovie> scoredMovies = new ArrayList<>();
        File initialFile = new File("score-movies.txt");
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

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        final SpecificAvroSerializer<ScoredMovie> scoredMovieSerializer = new SpecificAvroSerializer<>();
        scoredMovieSerializer.configure(serdeConfig, false);

        final KafkaProducer<String, ScoredMovie> scoredMovieProducer = new KafkaProducer<>(props,
                Serdes.String().serializer(),
                scoredMovieSerializer);

        //sending movies every INPUT_THROUGHPUT ms
        scoredMovies.forEach(movie -> {
            scoredMovieProducer.send(new ProducerRecord<String, ScoredMovie>(INPUT_TOPIC, String.valueOf(movie.getId()), movie));
            try {
                Thread.sleep(INPUT_THROUGHPUT);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
