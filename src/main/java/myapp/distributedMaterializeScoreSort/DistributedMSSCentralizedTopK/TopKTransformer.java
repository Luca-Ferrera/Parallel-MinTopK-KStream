package myapp.distributedMaterializeScoreSort.DistributedMSSCentralizedTopK;

import myapp.avro.ScoredMovie;
import myapp.punctuators.TopKPunctuator;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Duration;

public class TopKTransformer implements Transformer<String, ScoredMovie, KeyValue<String,ScoredMovie>> {
    private KeyValueStore<String, ValueAndTimestamp<ScoredMovie>> state1;
    private KeyValueStore<String, ValueAndTimestamp<ScoredMovie>> state2;
    private int k;
    private String storeName1;
    private String storeName2;

    public TopKTransformer(int k, String storeName1) {
        this.k = k;
        this.storeName1 = storeName1;
        this.storeName2 = "";
    }

    public TopKTransformer(int k, String storeName1, String storeName2) {
        this.k = k;
        this.storeName1 = storeName1;
        this.storeName2 = storeName2;
    }

    public void init(ProcessorContext context) {
        this.state1 = (KeyValueStore<String, ValueAndTimestamp<ScoredMovie>>) context.getStateStore(this.storeName1);
        if(!this.storeName2.isEmpty()) {
            this.state2 = (KeyValueStore<String, ValueAndTimestamp<ScoredMovie>>) context.getStateStore(this.storeName2);
            context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, new TopKPunctuator(this.state1, this.state2, context, k));
        } else {
            // punctuate each 10 second, can access this.state
            context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, new TopKPunctuator(this.state1, context, k));
        }
    }

    public  KeyValue<String,ScoredMovie> transform(String key, ScoredMovie value) {
        // can access this.state
        return null;
    }

    public void close() {
        // can access this.state
    }
}
