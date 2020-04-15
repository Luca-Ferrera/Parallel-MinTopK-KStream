package myapp.exercises;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import serde.UserSerde;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class UserStreams {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-users");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, UserSerde.class);

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, User> source = builder.stream("streams-users-input");
        source.filter((__, user) -> user.getGender().equals("MALE"))
                .mapValues(new ValueMapper<User, User>() {
                    @Override
                    public User apply(User u) {
                        User user = new User(u.getRegistertime(), u.getUserid(), u.getRegionid(), u.getGender());
                        user.setGender("FEMALE");
                        return user;
                    }
                })
                .to("streams-users-output", Produced.with(Serdes.String(), new UserSerde()));

        Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
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
