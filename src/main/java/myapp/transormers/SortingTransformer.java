package myapp.transormers;

import myapp.punctuators.SortingPunctuator;
import myapp.avro.ScoredMovie;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Duration;

public class SortingTransformer implements Transformer<String, ScoredMovie, ScoredMovie> {
    private KeyValueStore<String, ValueAndTimestamp<ScoredMovie>> state;

    public void init(ProcessorContext context) {
        this.state = (KeyValueStore<String, ValueAndTimestamp<ScoredMovie>>) context.getStateStore("scored-movies");
        // punctuate each 10 second, can access this.state
        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, new SortingPunctuator(this.state, context));
    }

    public ScoredMovie transform(String key, ScoredMovie value) {
        // can access this.state
        return value;
    }

    public void close() {
        // can access this.state
    }
}
