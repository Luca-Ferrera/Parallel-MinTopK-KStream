package myapp.distributedMinTopK;

import myapp.avro.MinTopKEntry;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class CentralizedTopKTransformer implements Transformer<Long, MinTopKEntry,  KeyValue<Long, MinTopKEntry>> {

    private final Boolean cleanDataStructure;
    private final int k;
    private KeyValueStore<Long, ArrayList<MinTopKEntry>> windowedTopKState;
    private ProcessorContext context;
    private int INSTANCE_NUMBER = 6;

    public CentralizedTopKTransformer(int k, String cleanDataStructure) {
        this.cleanDataStructure = cleanDataStructure.equals("clean");
        this.k = k;
    }

    public void init(ProcessorContext context) {
        this.context = context;
    }

    public KeyValue<Long,MinTopKEntry> transform(Long key, MinTopKEntry value) {
        this.windowedTopKState = (KeyValueStore<Long, ArrayList<MinTopKEntry>>) context.getStateStore("windowed-topk-store");
        if(this.cleanDataStructure){
            this.windowedTopKState.all().forEachRemaining(elem -> windowedTopKState.delete(elem.key));
            System.out.println("CLEANED");
            return null;
        }
        // insert new record in the store based on its key (windowID)
        this.windowedTopKState.putIfAbsent(key, new ArrayList<MinTopKEntry>());
        ArrayList<MinTopKEntry> windowArray = this.windowedTopKState.get(key);
        windowArray.add(value);
        this.windowedTopKState.put(key, windowArray);
        if(windowArray.size() == k * INSTANCE_NUMBER) {
            Comparator<MinTopKEntry> compareByScore = Comparator.comparingDouble(MinTopKEntry::getScore);
            Collections.sort(windowArray, compareByScore.reversed());
            List<MinTopKEntry> subList = windowArray.subList(0,k);
            // remove the list associated to windowID=key because from now on it'll be useless
            this.windowedTopKState.delete(key);
            // forward topK records for the windowID=key
            subList.forEach(elem -> {
                this.context.forward(key, elem);
            });
        }
        return null;
    }

    public void close() {
        // can access this.state
    }
}
