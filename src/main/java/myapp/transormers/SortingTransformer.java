package myapp.transormers;

import myapp.avro.ScoredMovie;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class SortingTransformer implements Transformer<String, ScoredMovie,  KeyValue<Long, ScoredMovie>> {

    private final Boolean cleanDataStructure;
    private KeyValueStore<String, ValueAndTimestamp<ScoredMovie>> state;
    private KeyValueStore<Integer, Integer> countState;
    private ProcessorContext context;
    private final int SIZE = 1000;
    private final int HOPPING_SIZE = 200;

    public SortingTransformer(String cleanDataStructure) {
        this.cleanDataStructure = cleanDataStructure.equals("clean");
    }

    public void init(ProcessorContext context) {
        this.context = context;
        this.state = (KeyValueStore<String, ValueAndTimestamp<ScoredMovie>>) context.getStateStore("scored-movies");
        this.countState = (KeyValueStore<Integer, Integer>) context.getStateStore("record-count-store");
        // punctuate each 10 second, can access this.state
//        context.schedule(Duration.ofMillis(1), PunctuationType.WALL_CLOCK_TIME, new SortingPunctuator(this.state, context));
    }

    public KeyValue<Long,ScoredMovie> transform(String key, ScoredMovie value) {
        // can access this.state
        this.state = (KeyValueStore<String, ValueAndTimestamp<ScoredMovie>>) context.getStateStore("scored-movies");
        this.countState = (KeyValueStore<Integer, Integer>) context.getStateStore("record-count-store");
        if(this.cleanDataStructure){
            this.countState.delete(-1);
            System.out.println("CLEANED");
            return null;
        }
        System.out.println("The offset of the record " + key + " we just read is: " + this.context.offset());
        int recordCount = this.countState.all().hasNext() ? this.countState.get(-1) : 0;
        if(recordCount > SIZE) {
            this.state.delete(Integer.toString(recordCount-SIZE-1));
        }
        if(recordCount >= SIZE && recordCount % HOPPING_SIZE == 1){
            ArrayList<KeyValue<String, ScoredMovie>> scoreList = new ArrayList<>();
            this.state.all().forEachRemaining((elem)-> {
                KeyValue<String,ScoredMovie> entry = new KeyValue<>(elem.key, elem.value.value());
                scoreList.add(entry);
            });
            Comparator<KeyValue<String, ScoredMovie>> compareByScore = Comparator.comparingDouble((KeyValue<String, ScoredMovie> o) -> o.value.getScore());
            Collections.sort(scoreList, compareByScore.reversed());
            long newKey = recordCount/HOPPING_SIZE - SIZE/HOPPING_SIZE;
            scoreList.forEach(elem -> context.forward(newKey, elem.value));
        }
        recordCount ++;
        this.countState.put(-1, recordCount);
        return null;
    }

    public void close() {
        // can access this.state
    }
}
