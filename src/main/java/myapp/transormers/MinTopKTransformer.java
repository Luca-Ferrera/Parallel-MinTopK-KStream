package myapp.transormers;

import myapp.avro.MinTopKEntry;
import myapp.avro.PhysicalWindow;
import myapp.avro.ScoredMovie;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class MinTopKTransformer implements Transformer<String, ScoredMovie, KeyValue<String,ScoredMovie>> {
    private KeyValueStore<String, ValueAndTimestamp<ScoredMovie>> state1;
    private KeyValueStore<String, ValueAndTimestamp<ScoredMovie>> state2;
    private KeyValueStore<Double, ValueAndTimestamp<PhysicalWindow>> physicalWindowsStore;
    private int k;
//    private String storeName1;
//    private String storeName2;
    private ProcessorContext context;
    private final int SIZE = 20;
    private final int HOPPING_SIZE = 5;
    private List<MinTopKEntry> superTopKList;
    private List<PhysicalWindow> lowerBoundPointer;
    private MinTopKEntry lastEntry;

//    public MinTopKTransformer(int k, String storeName1) {
//        this.k = k;
//        this.storeName1 = storeName1;
//        this.storeName2 = "";
//    }

    public MinTopKTransformer(int k, String storeName1, String storeName2) {
        this.k = k;
//        this.storeName1 = storeName1;
//        this.storeName2 = storeName2;
        this.superTopKList = new ArrayList<>();
        this.lowerBoundPointer = new LinkedList<>();
    }

    public void init(ProcessorContext context) {
        this.context = context;
//        this.state1 = (KeyValueStore<String, ValueAndTimestamp<ScoredMovie>>) context.getStateStore(this.storeName1);
//        if(!this.storeName2.isEmpty()) {
//            this.state2 = (KeyValueStore<String, ValueAndTimestamp<ScoredMovie>>) context.getStateStore(this.storeName2);
//            context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, new TopKPunctuator(this.state1, this.state2, context, k));
//        } else {
//            // punctuate each 10 second, can access this.state
//            context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, new TopKPunctuator(this.state1, context, k));
//        }
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
            MinTopKEntry firstEntry = new MinTopKEntry((double) value.getId(), value.getScore(),
                                            0.0, (double) SIZE/HOPPING_SIZE);
            this.superTopKList.add(firstEntry);
            PhysicalWindow  startingWindow = new PhysicalWindow(0.0, SIZE, HOPPING_SIZE, 1, 1 ,firstEntry);
            physicalWindowsStore.put(startingWindow.getId(), ValueAndTimestamp.make(startingWindow, context.timestamp()));
            if( startingWindow.getActualRecords() == startingWindow.getHoppingSize()) {
                // case of HOPPING_SIZE == 1
                this.createNewWindow(startingWindow, firstEntry);
            }
        } else {
            updateSuperTopK(value);
            while(windowsIterator.hasNext()) {
                PhysicalWindow window = windowsIterator.next().value.value();
                window.increaseActualRecords(1);
                if(window.getActualRecords() == window.getSize()) {
                    // window ends
                    physicalWindowsStore.delete(window.getId());
                    this.handlingExpiration();
                    this.lowerBoundPointer.remove(window);
                    forwardTopK();
                }
                if(! windowsIterator.hasNext() && window.getActualRecords() == window.getHoppingSize()) {
                    // last window, create new window
                    this.createNewWindow(window, lastEntry); // change value with MinTopKEntry
                }
            }
        }
        return null;
    }

    private void createNewWindow(PhysicalWindow window, MinTopKEntry entry) {
        PhysicalWindow newWindow = new PhysicalWindow(window.getId() + 1.0, SIZE, HOPPING_SIZE, 1, 0, entry);
        physicalWindowsStore.put(newWindow.getId(), ValueAndTimestamp.make(newWindow, context.timestamp()));
    }

    private void handlingExpiration(){
        //TODO: for first topK elements in superTopKList:
        // [x] increase by 1 the startingWindow
        // [x] if startingWindow > endingWindow remove the elem from superTopKList
        Iterator<MinTopKEntry> superTopKIterator = this.superTopKList.iterator();
        int i = 0;
        while(superTopKIterator.hasNext() && i < this.k) {
            MinTopKEntry entry = superTopKIterator.next();
            entry.increaseStartingWindow(1.0);
            if(entry.getStartingWindow() > entry.getEndingWindow()) {
                this.superTopKList.remove(entry);
            }
            i++;
        }
    }

    private void updateSuperTopK(ScoredMovie movie){
        boolean flag = false;
        if(movie.getScore() < this.lastEntry.getScore() && everyWindowHasTopK()){
            return;
        }
        for(PhysicalWindow window: this.lowerBoundPointer){
            MinTopKEntry lowerBoundPointed = window.getLowerBoundPointer();
            if(lowerBoundPointed.getScore() < movie.getScore()){
                if(!flag) {
                    MinTopKEntry newEntry = new MinTopKEntry((double) movie.getId(), movie.getScore(), window.getId(),
                            window.getId() + (double) SIZE / HOPPING_SIZE);
                    //insert newEntry in superTopKList
                    this.superTopKList.add(this.superTopKList.indexOf(lowerBoundPointed), newEntry);
                    flag = true;
                }
                if(window.getTopKCounter() < this.k){
                    window.increaseTopKCounter(1);
                } else {
                    lowerBoundPointed.increaseStartingWindow(1.0);
                }
                if(lowerBoundPointed.getStartingWindow() > lowerBoundPointed.getEndingWindow()) {
                    //remove from superTopKList
                    int index = this.superTopKList.indexOf(lowerBoundPointed);
                    this.superTopKList.remove(index);
                    window.setLowerBoundPointer(this.superTopKList.get(index + 1));
                }
            }
        }
        //update lastEntry
        this.lastEntry = this.superTopKList.get(this.superTopKList.size() -1);
    }

    private boolean everyWindowHasTopK(){
        for(PhysicalWindow window: this.lowerBoundPointer){
            if(window.getTopKCounter() != this.k){
                return false;
            }
        }
        return true;
    }
    /*
     * forward records as (key:windowId, value:movie.id)
     */
    private void forwardTopK(){
       List<MinTopKEntry> topK = this.superTopKList.subList(0,this.k-1);
       topK.forEach(elem -> this.context.forward(elem.getEndingWindow(),elem.getId()));
    }

    public void close() {
        // can access this.state
    }
}

