package myapp.transormers;

import myapp.avro.PhysicalWindow;
import myapp.avro.ScoredMovie;
import myapp.punctuators.TopKPunctuator;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Duration;

public class MinTopKTransformer implements Transformer<String, ScoredMovie, KeyValue<String,ScoredMovie>> {
    private KeyValueStore<String, ValueAndTimestamp<ScoredMovie>> state1;
    private KeyValueStore<String, ValueAndTimestamp<ScoredMovie>> state2;
    private KeyValueStore<Double, ValueAndTimestamp<PhysicalWindow>> physicalWindowsStore;
    private int k;
    private String storeName1;
    private String storeName2;
    private ProcessorContext context;
    private final int SIZE = 20;
    private final int HOPPING_SIZE = 5;

    public MinTopKTransformer(int k, String storeName1) {
        this.k = k;
        this.storeName1 = storeName1;
        this.storeName2 = "";
    }

    public MinTopKTransformer(int k, String storeName1, String storeName2) {
        this.k = k;
        this.storeName1 = storeName1;
        this.storeName2 = storeName2;
    }

    public void init(ProcessorContext context) {
        this.context = context;
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
        //TODO: STORE ONLY ACTIVE WINDOW
        // [x] take all windows
        // [x] check if one expire, in that case remove and forward the topK
        // [x] check if new need to be added
        // [] update currentWindow ID, maybe no need to keep the currentWindow ID
        physicalWindowsStore = (KeyValueStore<Double, ValueAndTimestamp<PhysicalWindow>>) context.getStateStore("windows-store");
        KeyValueIterator<Double, ValueAndTimestamp<PhysicalWindow>> windowsIterator = physicalWindowsStore.all();
        if(! windowsIterator.hasNext()) {
            PhysicalWindow  startingWindow = new PhysicalWindow(0.0, SIZE, HOPPING_SIZE, 1);
            //TODO: assign the record to the window
            physicalWindowsStore.put(startingWindow.getId(), ValueAndTimestamp.make(startingWindow, context.timestamp()));
            if( startingWindow.getActualRecords() == startingWindow.getHoppingSize()) {
                // case of HOPPING_SIZE == 1
                this.creteNewWindow(startingWindow);
            }
        } else {
            while(windowsIterator.hasNext()) {
                //TODO: assign the record to the window
                PhysicalWindow window = windowsIterator.next().value.value();
                window.increaseActualRecords(1);
                if(window.getActualRecords() == window.getSize()) {
                    // window ends
                    physicalWindowsStore.delete(window.getId());
                    //TODO: forward topK
                }
                if(! windowsIterator.hasNext() && window.getActualRecords() == window.getHoppingSize()) {
                    // last window, create new window
                    this.creteNewWindow(window);
                }
            }
        }
        return null;
    }

    private void creteNewWindow(PhysicalWindow window) {
        PhysicalWindow newWindow = new PhysicalWindow(window.getId() + 1.0, SIZE, HOPPING_SIZE, 1);
        //TODO: assign the record to newWindow
        physicalWindowsStore.put(newWindow.getId(), ValueAndTimestamp.make(newWindow, context.timestamp()));
    }
    public void close() {
        // can access this.state
    }
}

