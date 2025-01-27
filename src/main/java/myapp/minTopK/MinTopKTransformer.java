package myapp.minTopK;

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
import java.util.stream.Collectors;

import static java.lang.Integer.min;

public class MinTopKTransformer implements Transformer<String, ScoredMovie, KeyValue<Long,MinTopKEntry>> {
    private KeyValueStore<Integer, MinTopKEntry> superTopKListStore;
    private KeyValueStore<Long, PhysicalWindow> physicalWindowsStore;
    private final int k;
    private final Boolean cleanDataStructure;
    private ProcessorContext context;
    private final int SIZE = 3600;
    private final int HOPPING_SIZE = 300;
    private ArrayList<MinTopKEntry> superTopKList;
    private LinkedList<PhysicalWindow> lowerBoundPointer;
    private PhysicalWindow currentWindow;
    private PhysicalWindow lastWindow;

    public MinTopKTransformer(int k, String cleanDataStructure) {
        this.k = k;
        this.cleanDataStructure = cleanDataStructure.equals("clean");
    }

    public void init(ProcessorContext context) {
        this.context = context;
    }

    public  KeyValue<Long,MinTopKEntry> transform(String key, ScoredMovie value) {
        //STORE ONLY ACTIVE WINDOW
        // [x] take all windows
        // [x] check if one expire, in that case remove and forward the topK
        // [x] check if new need to be added
        // [x] update currentWindow ID
        physicalWindowsStore = (KeyValueStore<Long, PhysicalWindow>) context.getStateStore("windows-store");
        superTopKListStore = (KeyValueStore<Integer, MinTopKEntry>) context.getStateStore("super-topk-list-store");
        //clear data structure
        if(this.cleanDataStructure){
            System.out.println("Cleaning Data Structures");
            physicalWindowsStore.all().forEachRemaining(elem -> physicalWindowsStore.delete(elem.key));
            superTopKListStore.all().forEachRemaining(elem -> superTopKListStore.delete(elem.key));
            return null;
        }
        setUpDataStructures();
        KeyValueIterator<Long, PhysicalWindow> windowsIterator = physicalWindowsStore.all();
        MinTopKEntry newEntry = null;
        if(!windowsIterator.hasNext()) {
            //create first window
            MinTopKEntry firstEntry = new MinTopKEntry(value.getId(), value.getScore(),
                                            0L, (long) Math.ceil((0.0 / HOPPING_SIZE) - 1));
            this.superTopKList.add(firstEntry);
            PhysicalWindow  startingWindow = new PhysicalWindow(0L, SIZE, HOPPING_SIZE, 1, 1 ,firstEntry);
            physicalWindowsStore.put(startingWindow.getId(), startingWindow);
            physicalWindowsStore.put(-1L, startingWindow);
            physicalWindowsStore.put(-2L, startingWindow);
            this.lowerBoundPointer.add(startingWindow);
            this.currentWindow = startingWindow;
            this.lastWindow = startingWindow;
            if(startingWindow.getActualRecords() == startingWindow.getHoppingSize())
                // case of HOPPING_SIZE == 1
                this.createNewWindow(firstEntry);
        } else {
            //Skip currentWindow (key=-1L) && lastWindow (key=-2L)
            physicalWindowsStore.delete(-1L);
            physicalWindowsStore.delete(-2L);
            windowsIterator = physicalWindowsStore.all();
            while(windowsIterator.hasNext()) {
                KeyValue<Long, PhysicalWindow> keyValue = windowsIterator.next();
                PhysicalWindow window = keyValue.value;
//                System.out.println("ADDING RECORD TO " + window);
                int index = this.lowerBoundPointer.indexOf(window);
                window.increaseActualRecords(1);
                //update window in lowerBoundPointer
                this.lowerBoundPointer.set(index, window);
                // update StateStore
                physicalWindowsStore.put(window.getId(), window);
                if(window.getActualRecords() == window.getSize() + 1) {
                    // window ends
                    physicalWindowsStore.delete(window.getId());
                    forwardTopK(window.getId());
                    this.handlingExpiration();
                    this.lowerBoundPointer.remove(window);
                    //updating currentWindow
                    this.currentWindow = this.physicalWindowsStore.get(window.getId() + 1L);
                    this.physicalWindowsStore.put(-1L, this.currentWindow);
                    //TODO: not sure if it is correct, I'm removing the element from superTopK if its endingWindow is expired
                    // maybe this case should not happen but it is happening
                    this.superTopKList.removeIf(entry -> entry.getEndingWindow() == window.getId());
                }
                if(window.getId()== this.currentWindow.getId() && window.getActualRecords() != 1 && window.getActualRecords() % window.getHoppingSize() == 1) {
                    // last window, create new window
                    //TODO: fix endingWindow computation as in branch MinTopK+N
                    newEntry = new MinTopKEntry(value.getId(), value.getScore(),
                            this.currentWindow.getId(), (long) Math.ceil(((double)this.currentWindow.getActualRecords() / HOPPING_SIZE)-1));
                    this.createNewWindow(newEntry);
                }
            }
            updateSuperTopK(value, newEntry);
        }
        windowsIterator.close();
        saveDataStructures();
        return null;
    }

    private void createNewWindow(MinTopKEntry entry) {
        PhysicalWindow newWindow;
//        PhysicalWindow lastWindow = physicalWindowsStore.get(-2L);
        if(entry.getScore() < this.superTopKList.get(this.superTopKList.size() -1).getScore() && everyWindowHasTopK())
            //entry won't be added to superTopKList so it can't be the LowerBoundPointer
            newWindow = new PhysicalWindow(this.lastWindow.getId() + 1L, SIZE, HOPPING_SIZE, 1,1, this.superTopKList.get(this.superTopKList.size() -1));
        else
            //if entry.Score <= lastEntry.Score ==> entry will be the new lastEntry in superTopKList
            newWindow = new PhysicalWindow(this.lastWindow.getId() + 1L, SIZE, HOPPING_SIZE, 1, 1,
                    entry.getScore() <= this.superTopKList.get(this.superTopKList.size() -1).getScore() ? entry : this.superTopKList.get(this.superTopKList.size() -1));
        //save new window
        physicalWindowsStore.put(newWindow.getId(), newWindow);
        physicalWindowsStore.put(-2L, newWindow);
        this.lastWindow = newWindow;
        this.lowerBoundPointer.add(newWindow);
    }

    private void handlingExpiration(){
        // for first topK elements in superTopKList:
        // - increase by 1 the startingWindow
        // - if startingWindow > endingWindow remove the elem from superTopKList
        Iterator<MinTopKEntry> superTopKIterator = this.superTopKList.iterator();
        int i = 0;
        while(superTopKIterator.hasNext() && i < this.k) {
            MinTopKEntry entry = superTopKIterator.next();
            entry.increaseStartingWindow(1L);
            if(entry.getStartingWindow() > entry.getEndingWindow()) {
//                System.out.println("REMOVING " + entry);
                superTopKIterator.remove();
                this.superTopKListStore.delete(i);
            }
            i++;
        }
    }

    private void updateSuperTopK(ScoredMovie movie, MinTopKEntry topKEntry){
        if(movie.getScore() < this.superTopKList.get(this.superTopKList.size() -1).getScore() && everyWindowHasTopK()){
            return;
        }
        //add new record to superTopKList
        MinTopKEntry newEntry = topKEntry == null ?
                new MinTopKEntry(movie.getId(), movie.getScore(), this.currentWindow.getId(), (long) Math.ceil(((double)this.currentWindow.getActualRecords() / HOPPING_SIZE)-1))
                : topKEntry;
        //insert newEntry in superTopKList
        insertNewEntry(newEntry);
        for(PhysicalWindow window: this.lowerBoundPointer){
            MinTopKEntry lowerBoundPointed = window.getLowerBoundPointer();
            //case that topKCounter < K and new record score < lowerBound score --> new record become lowerBound
            if(lowerBoundPointed.getScore() >= movie.getScore() && window.getTopKCounter() < this.k){
                window.setLowerBoundPointer(newEntry);
                window.increaseTopKCounter(1);
                this.physicalWindowsStore.put(window.getId(), window);
            }
            int index = this.superTopKList.indexOf(lowerBoundPointed);
            if(index == -1){
                //lowerBoundPointed removed previously from the topKList, update lowerBound to the last element of topKList
                MinTopKEntry newLowerBound = this.superTopKList.get(this.superTopKList.size() - 1);
                window.setLowerBoundPointer(newLowerBound);
                this.physicalWindowsStore.put(window.getId(), window);
            } else if(lowerBoundPointed.getScore() < movie.getScore()){
                if(window.getTopKCounter() < this.k){
                    window.increaseTopKCounter(1);
                    this.physicalWindowsStore.put(window.getId(), window);
                } else {
                    //increase starting window
                    lowerBoundPointed.increaseStartingWindow(1L);
                    window.setLowerBoundPointer(lowerBoundPointed);
                    if(lowerBoundPointed.getStartingWindow() > lowerBoundPointed.getEndingWindow()) {
                        //remove from superTopKList
                        this.superTopKList.remove(index);
                        this.superTopKListStore.delete(index);
                    } else{
                        //update lowerBoundPointed into superTopKList
                        this.superTopKList.set(index, lowerBoundPointed);
                        this.superTopKListStore.put(index, lowerBoundPointed);
                    }
                    //move lowerBoundPointer one position up in the superTopKList
                    MinTopKEntry newLowerBound = this.superTopKList.get(index - 1);
                    window.setLowerBoundPointer(newLowerBound);
                    this.physicalWindowsStore.put(window.getId(), window);
                }
            }
        }
        //Update the superTopKList element accordingly to the updates in the lowerBoundPointer
        for (MinTopKEntry entry : this.superTopKList) {
            for (PhysicalWindow window : this.lowerBoundPointer) {
                MinTopKEntry lowerBoundPointed = window.getLowerBoundPointer();
                //check if it's the same lowerBound but with startingWindow increased by 1
                if (entry.sameButIncreasedStartingWindow(lowerBoundPointed)) {
                    entry.increaseStartingWindow(1L);
                    break;
                }
            }
        }
    }

    private void insertNewEntry(MinTopKEntry newEntry){
        int size = this.superTopKList.size();
        for (int i = 0; i < size; i++) {
            MinTopKEntry elem = this.superTopKList.get(i);
            // if the element equals newEntry, return, because we don't add duplicates
            if (elem.equals(newEntry)) return;
            // if the element score you are looking at is bigger than newEntry score, go to the next element
            if (elem.getScore() >= newEntry.getScore()) continue;
            // otherwise, we have found the location to add newEntry
            this.superTopKList.add(i, newEntry);
            return;
        }
        //add newEntry at the end of superTopKList
        this.superTopKList.add(newEntry);
    }

    private boolean everyWindowHasTopK(){
        for(PhysicalWindow window: this.lowerBoundPointer){
            if(window.getTopKCounter() != this.k)
                return false;
        }
        return true;
    }

    /*
     * forward records as (key:windowId, value:minTopKEntry)
     */
    private void forwardTopK(long windowId){
       List<MinTopKEntry> topK = this.superTopKList.subList(0,min(this.superTopKList.size(),this.k));
       topK.forEach(elem -> {
           this.context.forward(windowId ,elem);
       });
    }

    private void setUpDataStructures(){
        this.superTopKList = new ArrayList<MinTopKEntry>();
        KeyValueIterator<Integer, MinTopKEntry> superTopKIterator = this.superTopKListStore.all();
        while(superTopKIterator.hasNext()) {
            KeyValue<Integer, MinTopKEntry> keyValue = superTopKIterator.next();
            if(keyValue.key <= this.superTopKList.size()) {
                this.superTopKList.add(keyValue.key, keyValue.value);
            }
            else
                this.superTopKList.add(keyValue.value);
        }
        superTopKIterator.close();
        this.lowerBoundPointer = new LinkedList<>();
        KeyValueIterator<Long, PhysicalWindow> lowerBoundPointerIterator = this.physicalWindowsStore.all();
        while(lowerBoundPointerIterator.hasNext()){
            KeyValue<Long, PhysicalWindow> keyValue = lowerBoundPointerIterator.next();
            if(keyValue.key <= this.lowerBoundPointer.size()) {
                if(keyValue.key == -1) {
                    this.currentWindow = keyValue.value;
                }
                else if(keyValue.key == -2){
                    this.lastWindow = keyValue.value;
                }
                else{
                    this.lowerBoundPointer.add(Math.toIntExact(keyValue.key), keyValue.value);
                }
            }
            else
                this.lowerBoundPointer.add(keyValue.value);
        }
        lowerBoundPointerIterator.close();
    }

    private void saveDataStructures(){
        final Integer[] i = {0};
        KeyValueIterator<Integer, MinTopKEntry> superTopKIterator = this.superTopKListStore.all();
        while(superTopKIterator.hasNext()) {
            Integer keyValue = superTopKIterator.next().key;
            this.superTopKListStore.delete(keyValue);
        }
        this.superTopKList.forEach(elem -> {
            this.superTopKListStore.put(i[0], elem);
            i[0]++;
        });
        this.lowerBoundPointer.forEach(elem -> {
            this.physicalWindowsStore.put(elem.getId(), elem);
        });
        this.physicalWindowsStore.put(-1L, this.currentWindow);
        this.physicalWindowsStore.put(-2L, this.lastWindow);
    }

    public void close() {
        // can access this.state
    }
}

