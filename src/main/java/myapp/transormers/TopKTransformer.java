package myapp.transormers;

import myapp.avro.ScoredMovie;
import myapp.punctuators.TopKPunctuator;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Duration;

public class TopKTransformer implements Transformer<String, ScoredMovie, ScoredMovie> {
    private KeyValueStore<String, ValueAndTimestamp<ScoredMovie>> state;
    private int k;
    private String storeName;
    public TopKTransformer(int k, String storeName) {
        this.k = k;
        this.storeName = storeName;
    }

    public void init(ProcessorContext context) {
        this.state = (KeyValueStore<String, ValueAndTimestamp<ScoredMovie>>) context.getStateStore(this.storeName);
        // punctuate each 10 second, can access this.state
        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, new TopKPunctuator(this.state, context, k));
    }

    public ScoredMovie transform(String key, ScoredMovie value) {
        // can access this.state
        return value;
    }

    public void close() {
        // can access this.state
    }
}
