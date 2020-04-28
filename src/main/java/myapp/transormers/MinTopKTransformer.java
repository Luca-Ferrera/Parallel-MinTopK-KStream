package myapp.transormers;

import myapp.avro.MinTopKEntry;
import myapp.avro.PhysicalWindow;
import myapp.avro.ScoredMovie;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class MinTopKTransformer implements Transformer<String, ScoredMovie, KeyValue<String,Long>> {
//    private KeyValueStore<Integer,PhysicalWindow> lowerBoundPointerStore;
    private KeyValueStore<Integer, MinTopKEntry> superTopKListStore;
    private KeyValueStore<Long, PhysicalWindow> physicalWindowsStore;
    private int k;
//    private String storeName1;
//    private String storeName2;
    private ProcessorContext context;
    private final int SIZE = 20;
    private final int HOPPING_SIZE = 5;
    private List<MinTopKEntry> superTopKList;
    private List<PhysicalWindow> lowerBoundPointer;
    private MinTopKEntry lastEntry;
    private PhysicalWindow currentWindow;

    public MinTopKTransformer(int k) {
        this.k = k;
//        this.superTopKList = new ArrayList<>();
//        this.lowerBoundPointer = new LinkedList<>();
    }

    public void init(ProcessorContext context) {
        this.context = context;
    }

    public  KeyValue<String,Long> transform(String key, ScoredMovie value) {
        //TODO: STORE ONLY ACTIVE WINDOW
        // [x] take all windows
        // [x] check if one expire, in that case remove and forward the topK
        // [x] check if new need to be added
        // [x] update currentWindow ID
        System.out.println("TRANSFORM KEY: " + key + " VALUE: " + value);
        physicalWindowsStore = (KeyValueStore<Long, PhysicalWindow>) context.getStateStore("windows-store");
        superTopKListStore = (KeyValueStore<Integer, MinTopKEntry>) context.getStateStore("super-topk-list-store");
//        lowerBoundPointerStore = (KeyValueStore<Integer, PhysicalWindow>) context.getStateStore("lower-bound-pointer-store");
//        lowerBoundPointerStore.all().forEachRemaining(elem -> lowerBoundPointerStore.delete(elem.key));

//        physicalWindowsStore.all().forEachRemaining(elem -> physicalWindowsStore.delete(elem.key));
//        superTopKListStore.all().forEachRemaining(elem -> superTopKListStore.delete(elem.key));
//        return null;
        setUpDataStructures();
        KeyValueIterator<Long, PhysicalWindow> windowsIterator = physicalWindowsStore.all();
        if(!windowsIterator.hasNext()) {
            System.out.println("EMPTY WINDOWS STORE");
            MinTopKEntry firstEntry = new MinTopKEntry(value.getId(), value.getScore(),
                                            0L, + (long) SIZE/HOPPING_SIZE);
            this.superTopKList.add(firstEntry);
            PhysicalWindow  startingWindow = new PhysicalWindow(0L, SIZE, HOPPING_SIZE, 1, 1 ,firstEntry);
            physicalWindowsStore.put(startingWindow.getId(), startingWindow);
            physicalWindowsStore.put(-1L, startingWindow);
            this.lowerBoundPointer.add(startingWindow);
            this.currentWindow = startingWindow;
            this.lastEntry = firstEntry;
            if(startingWindow.getActualRecords() == startingWindow.getHoppingSize()) {
                // case of HOPPING_SIZE == 1
                this.createNewWindow(startingWindow, firstEntry);
            }
        } else {
//            System.out.println("WINDOW " + windowsIterator.next());
//            System.out.println("LAST ENTRY " + superTopKListStore.get(-1));
            //Skip currentWindow
            physicalWindowsStore.delete(-1L);
            windowsIterator = physicalWindowsStore.all();
            while(windowsIterator.hasNext()) {
                KeyValue<Long, PhysicalWindow> keyValue = windowsIterator.next();
                PhysicalWindow window = keyValue.value;
                System.out.println("ADDING RECORD TO " + window);
                int index = this.lowerBoundPointer.indexOf(window);
                window.increaseActualRecords(1);
                //update window in lowerBoundPointer
                this.lowerBoundPointer.set(index, window);
                // update StateStore
                physicalWindowsStore.put(window.getId(), window);
                if(window.getActualRecords() == window.getSize() + 1) {
                    // window ends
                    physicalWindowsStore.delete(window.getId());
                    this.handlingExpiration();
//                    lowerBoundPointerStore.delete(window.getId());
                    this.lowerBoundPointer.remove(window);
                    //TODO: check if need of deleting window from StateStore
                    forwardTopK();
                }
                if(window.getId()==this.currentWindow.getId() && window.getActualRecords() == window.getHoppingSize() + 1) {
                    // last window, create new window
                    //TODO: check what happen if new record should be on topKList and the use of lastEntry
                    System.out.println("Creating new window");
                    MinTopKEntry newEntry = new MinTopKEntry(value.getId(), value.getScore(),
                            this.currentWindow.getId(), this.currentWindow.getId() + (long) SIZE/HOPPING_SIZE);
//                    this.createNewWindow(window, lastEntry);
                    this.createNewWindow(this.currentWindow, newEntry);
                }
            }
            System.out.println("SUPERTOPK BEFORE: " + superTopKList);
            updateSuperTopK(value);
            System.out.println("SUPERTOPK AFTER: " + superTopKList);
        }
        windowsIterator.close();
        saveDataStructures();
        return null;
    }

    private void createNewWindow(PhysicalWindow window, MinTopKEntry entry) {
        PhysicalWindow newWindow;
        if(entry.getScore() < this.lastEntry.getScore() && everyWindowHasTopK()) {
            //entry won't be added to superTopKList so it can't the LowerBoundPointer
            newWindow = new PhysicalWindow(window.getId() + 1L, SIZE, HOPPING_SIZE, 1, 1, this.lastEntry);
        } else {
             newWindow = new PhysicalWindow(window.getId() + 1L, SIZE, HOPPING_SIZE, 1, 1, entry);
        }
        System.out.println("Inserting Window " + newWindow);
        physicalWindowsStore.put(newWindow.getId(), newWindow);
        this.lowerBoundPointer.add(newWindow);
        //TODO: check if needed
//        physicalWindowsStore.put(-1L, newWindow);
        this.currentWindow = newWindow;
    }

    private void handlingExpiration(){
        //TODO: for first topK elements in superTopKList:
        // [x] increase by 1 the startingWindow
        // [x] if startingWindow > endingWindow remove the elem from superTopKList
        Iterator<MinTopKEntry> superTopKIterator = this.superTopKList.iterator();
        int i = 0;
        while(superTopKIterator.hasNext() && i < this.k) {
            MinTopKEntry entry = superTopKIterator.next();
            entry.increaseStartingWindow(1L);
            if(entry.getStartingWindow() > entry.getEndingWindow()) {
                this.superTopKList.remove(entry);
            }
            //TODO: check if need to update StateStore
            i++;
        }
    }

    private void updateSuperTopK(ScoredMovie movie){
        boolean flag = false;
        if(movie.getScore() < this.lastEntry.getScore() && everyWindowHasTopK()){
            return;
        }
//        KeyValueIterator<Long, PhysicalWindow> lowerBoundPointerIterator = this.lowerBoundPointerStore.all();
//        while(lowerBoundPointerIterator.hasNext()){
        System.out.println("BEFORE FOR ON LOWERBOUNDPOINTER " + this.lowerBoundPointer);
        for(PhysicalWindow window: this.lowerBoundPointer){
//            KeyValue<Long, PhysicalWindow> keyValue = lowerBoundPointerIterator.next();
//            PhysicalWindow window = keyValue.value;
            MinTopKEntry lowerBoundPointed = window.getLowerBoundPointer();
            System.out.println("WINDOW " + window.getId() + " LowerBoundPointed: " + lowerBoundPointed);
            if(lowerBoundPointed.getScore() < movie.getScore()){
                if(!flag) {
                    //TODO: [x] SET CURRENTWINDOW when new window is created
//                    MinTopKEntry newEntry = new MinTopKEntry(movie.getId(), movie.getScore(), window.getId(),
//                            window.getId() + (long) SIZE / HOPPING_SIZE);
                      MinTopKEntry newEntry = new MinTopKEntry(movie.getId(), movie.getScore(), this.currentWindow.getId(),
                              this.currentWindow.getId() + (long) SIZE / HOPPING_SIZE);
                    System.out.println("NEWENTRY " + newEntry);
                    //insert newEntry in superTopKList
                    insertNewEntry(newEntry);
//                    if(! this.superTopKList.contains(newEntry))
//                        this.superTopKList.add(this.superTopKList.indexOf(lowerBoundPointed), newEntry);
                    flag = true;
                }
                if(window.getTopKCounter() < this.k){
                    System.out.println("Increasing TopKCounter of " + window);
                    window.increaseTopKCounter(1);
                    System.out.println("Window after increasing TopKCounter " + window);
                    this.physicalWindowsStore.put(window.getId(), window);
                } else {
                    System.out.println("Increasing starting window of " + lowerBoundPointed);
                    int index = this.superTopKList.indexOf(lowerBoundPointed);
                    lowerBoundPointed.increaseStartingWindow(1L);
                    //TODO:
                    // [x] update lowerBoundPointerStore
                    // [x] update the lowerBoundPointer to the one before
                    // [x] update the superTopKList with the updated lowerBoundPointed
                    this.superTopKList.set(index, lowerBoundPointed);
                    window.setLowerBoundPointer(this.superTopKList.get(index - 1));
//                    this.lowerBoundPointerStore.put(this.lowerBoundPointer.indexOf(window), window);
                    this.physicalWindowsStore.put(window.getId(), window);
                }
                if(lowerBoundPointed.getStartingWindow() > lowerBoundPointed.getEndingWindow()) {
                    //remove from superTopKList
                    int index = this.superTopKList.indexOf(lowerBoundPointed);
                    System.out.println("lower POINTED " + lowerBoundPointed);
                    System.out.println("SUPERTOPK "+ this.superTopKList);
                    System.out.println("INDEX " + index);
                    this.superTopKList.remove(index);
                    System.out.println("New WINDOW " + window.getId() + " LowerBoundPointed: " + this.superTopKList.get(index + 1));
                    window.setLowerBoundPointer(this.superTopKList.get(index - 1));
                    //update window in StateStore
                    this.physicalWindowsStore.put(window.getId(), window);
                }
            }
        }
        //update lastEntry
        this.lastEntry = this.superTopKList.get(this.superTopKList.size() -1);
//        lowerBoundPointerIterator.close();
    }

    private void insertNewEntry(MinTopKEntry newEntry){
        int size = this.superTopKList.size();
        for (int i = 0; i < size; i++) {
            // if the element you are looking at is smaller than x,
            // go to the next element
            if (this.superTopKList.get(i).getScore() >= newEntry.getScore()) continue;
            // if the element equals x, return, because we don't add duplicates
            if (this.superTopKList.get(i) == newEntry) return;
            // otherwise, we have found the location to add x
            this.superTopKList.add(i, newEntry);
            return;
        }
    }

    private boolean everyWindowHasTopK(){
        for(PhysicalWindow window: this.lowerBoundPointer){
//        KeyValueIterator<Long, PhysicalWindow> lowerBoundPointerIterator = this.lowerBoundPointerStore.all();
//        while(lowerBoundPointerIterator.hasNext()){
//            KeyValue<Long, PhysicalWindow> keyValue = lowerBoundPointerIterator.next();
//            PhysicalWindow window = keyValue.value;
            if(window.getTopKCounter() != this.k){
//                lowerBoundPointerIterator.close();
                return false;
            }
        }
//        lowerBoundPointerIterator.close();
        return true;
    }
    /*
     * forward records as (key:windowId, value:movie.id)
     */
    private void forwardTopK(){
       List<MinTopKEntry> topK = this.superTopKList.subList(0,this.k-1);
       topK.forEach(elem -> {
           System.out.println("FORWARDING KEY: " + elem.getEndingWindow() +" VALUE: " + elem.getId() );
           this.context.forward(elem.getEndingWindow(),elem.getId());
       });
    }

    private void setUpDataStructures(){
        this.superTopKList = new ArrayList<MinTopKEntry>();
        KeyValueIterator<Integer, MinTopKEntry> superTopKIterator = this.superTopKListStore.all();
        while(superTopKIterator.hasNext()) {
            KeyValue<Integer, MinTopKEntry> keyValue = superTopKIterator.next();
            if(keyValue.key <= this.superTopKList.size()) {
                if(keyValue.key != -1)
                    this.superTopKList.add(keyValue.key, keyValue.value);
                else {
                    System.out.println("Saving " + keyValue.value);
                    this.lastEntry = keyValue.value;
                }
            }
            else {
                this.superTopKList.add(keyValue.value);
            }
        }
        superTopKIterator.close();
        this.lowerBoundPointer = new LinkedList<>();
//        KeyValueIterator<Integer, PhysicalWindow> lowerBoundPointerIterator = this.lowerBoundPointerStore.all();
        KeyValueIterator<Long, PhysicalWindow> lowerBoundPointerIterator = this.physicalWindowsStore.all();
        while(lowerBoundPointerIterator.hasNext()){
//            KeyValue<Integer, PhysicalWindow> keyValue = lowerBoundPointerIterator.next();
            KeyValue<Long, PhysicalWindow> keyValue = lowerBoundPointerIterator.next();
            if(keyValue.key <= this.lowerBoundPointer.size()) {
//                this.lowerBoundPointer.add(keyValue.key, keyValue.value);
                if(keyValue.key != -1) {
//                    System.out.println("1 ADDING " + keyValue.value + " TO " + this.lowerBoundPointer);
                    this.lowerBoundPointer.add(Math.toIntExact(keyValue.key), keyValue.value);
                }
                else
                    this.currentWindow = keyValue.value;
            }
            else {
//                System.out.println("2 ADDING " + keyValue.value + " TO " + this.lowerBoundPointer);
                this.lowerBoundPointer.add(keyValue.value);
            }
        }
        lowerBoundPointerIterator.close();
    }

    private void saveDataStructures(){
        final Integer[] i = {0};
        this.superTopKList.forEach(elem -> {
            this.superTopKListStore.put(i[0], elem);
            i[0]++;
        });
        System.out.println("Saving lastEntry " + this.lastEntry);
        this.superTopKListStore.put(-1, this.lastEntry);
//        final Integer[] j = {0};
        final Long[] j = {0L};
        this.lowerBoundPointer.forEach(elem -> {
//            System.out.println("ADDING " + elem + " TO physicalWindowsStore " + j[0]);
//            this.lowerBoundPointerStore.put(j[0], elem);
            this.physicalWindowsStore.put(j[0], elem);
            j[0]++;
        });
        this.physicalWindowsStore.put(-1L, this.currentWindow);
    }

    public void close() {
        // can access this.state
    }
}

