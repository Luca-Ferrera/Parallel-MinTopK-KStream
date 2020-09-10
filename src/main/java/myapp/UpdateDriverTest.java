package myapp;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import myapp.avro.MovieIncome;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;

import java.io.*;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class UpdateDriverTest {

    static String INPUT_TOPIC = "movie-updates";
    static long INPUT_THROUGHPUT = 1500L;
    public static void main(final String [] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:29092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        System.out.println("Connecting to Kafka cluster via bootstrap servers " + bootstrapServers);
        System.out.println("Connecting to Confluent schema registry at " + schemaRegistryUrl);

        // Read comma-delimited file of songs into Array
        final List<MovieIncome> movieIncomes = new ArrayList<>();
        File initialFile = new File("updates-test.txt");
        InputStream targetStream = new FileInputStream(initialFile);
        final InputStreamReader streamReader = new InputStreamReader(targetStream, UTF_8);
        try (final BufferedReader br = new BufferedReader(streamReader)) {
            String line;
            while ((line = br.readLine()) != null) {
                final MovieIncome newMovieIncomes = new ObjectMapper().readValue(line, MovieIncome.class);
                movieIncomes.add(newMovieIncomes);
            }
        }

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class);

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        final SpecificAvroSerializer<MovieIncome> movieIncomeSerializer = new SpecificAvroSerializer<>();
        movieIncomeSerializer.configure(serdeConfig, false);

        final KafkaProducer<String, MovieIncome> movieIncomeProducer = new KafkaProducer<>(props,
                Serdes.String().serializer(),
                movieIncomeSerializer);

        //sending movies every INPUT_THROUGHPUT ms
        final Integer[] i = {0};
        movieIncomes.forEach(income -> {
            movieIncomeProducer.send(new ProducerRecord<String, MovieIncome>(INPUT_TOPIC, null, income),
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
