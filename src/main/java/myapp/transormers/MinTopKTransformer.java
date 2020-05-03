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

import static java.lang.Integer.min;

public class MinTopKTransformer implements Transformer<String, ScoredMovie, KeyValue<String,Long>> {
    private KeyValueStore<Integer, MinTopKEntry> superTopKListStore;
    private KeyValueStore<Long, PhysicalWindow> physicalWindowsStore;
    private int k;
    private ProcessorContext context;
    private final int SIZE = 8;
    private final int HOPPING_SIZE = 4;
    private ArrayList<MinTopKEntry> superTopKList;
    private LinkedList<PhysicalWindow> lowerBoundPointer;
    private PhysicalWindow currentWindow;

    public MinTopKTransformer(int k) {
        this.k = k;
    }

    public void init(ProcessorContext context) {
        this.context = context;
    }

    public  KeyValue<String,Long> transform(String key, ScoredMovie value) {
        //STORE ONLY ACTIVE WINDOW
        // [x] take all windows
        // [x] check if one expire, in that case remove and forward the topK
        // [x] check if new need to be added
        // [x] update currentWindow ID
        System.out.println("TRANSFORM KEY: " + key + " VALUE: " + value);
        physicalWindowsStore = (KeyValueStore<Long, PhysicalWindow>) context.getStateStore("windows-store");
        superTopKListStore = (KeyValueStore<Integer, MinTopKEntry>) context.getStateStore("super-topk-list-store");
        //clear data structure
//        physicalWindowsStore.all().forEachRemaining(elem -> physicalWindowsStore.delete(elem.key));
//        superTopKListStore.all().forEachRemaining(elem -> superTopKListStore.delete(elem.key));
//        return null;
        setUpDataStructures();
        KeyValueIterator<Long, PhysicalWindow> windowsIterator = physicalWindowsStore.all();
        if(!windowsIterator.hasNext()) {
            //create first window
            System.out.println("EMPTY WINDOWS STORE");
            MinTopKEntry firstEntry = new MinTopKEntry(value.getId(), value.getScore(),
                                            0L, + (long) SIZE / HOPPING_SIZE - 1L);
            this.superTopKList.add(firstEntry);
            PhysicalWindow  startingWindow = new PhysicalWindow(0L, SIZE, HOPPING_SIZE, 1, 1 ,firstEntry);
            physicalWindowsStore.put(startingWindow.getId(), startingWindow);
            physicalWindowsStore.put(-1L, startingWindow);
            this.lowerBoundPointer.add(startingWindow);
            this.currentWindow = startingWindow;
            if(startingWindow.getActualRecords() == startingWindow.getHoppingSize())
                // case of HOPPING_SIZE == 1
                this.createNewWindow(startingWindow, firstEntry);
        } else {
            //Skip currentWindow (key=-1L)
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
                    forwardTopK(window.getId());
                    this.handlingExpiration();
                    this.lowerBoundPointer.remove(window);
                }
                if(window.getId()== this.currentWindow.getId() && window.getActualRecords() == window.getHoppingSize() + 1) {
                    // last window, create new window
                    System.out.println("Creating new window");
                    MinTopKEntry newEntry = new MinTopKEntry(value.getId(), value.getScore(),
                            this.currentWindow.getId() + 1L, this.currentWindow.getId() + (long) SIZE / HOPPING_SIZE);
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
        // topKCounter = min(HOPPING_SIZE, this.k) and not = 0
        // because here I'm using physical windows instead of logical windows as in the paper
        if(entry.getScore() < this.superTopKList.get(this.superTopKList.size() -1).getScore() && everyWindowHasTopK())
            //entry won't be added to superTopKList so it can't be the LowerBoundPointer
            newWindow = new PhysicalWindow(window.getId() + 1L, SIZE, HOPPING_SIZE, 1, min(HOPPING_SIZE, this.k), this.superTopKList.get(this.superTopKList.size() -1));
        else
            //if entry.Score <= lastEntry.Score ==> entry will be the new lastEntry in superTopKList
            newWindow = new PhysicalWindow(window.getId() + 1L, SIZE, HOPPING_SIZE, 1, min(HOPPING_SIZE, this.k),
                    entry.getScore() <= this.superTopKList.get(this.superTopKList.size() -1).getScore() ? entry : this.superTopKList.get(this.superTopKList.size() -1));
        System.out.println("Inserting Window " + newWindow);
        //save new window
        physicalWindowsStore.put(newWindow.getId(), newWindow);
        this.lowerBoundPointer.add(newWindow);
        this.currentWindow = newWindow;
        this.physicalWindowsStore.put(-1L, newWindow);
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
                System.out.println("REMOVING " + entry);
                superTopKIterator.remove();
                this.superTopKListStore.delete(i);
            }
            i++;
        }
    }

    private void updateSuperTopK(ScoredMovie movie){
        boolean isNewEntryCreated = false;
        if(movie.getScore() < this.superTopKList.get(this.superTopKList.size() -1).getScore() && everyWindowHasTopK()){
            return;
        }
        System.out.println("BEFORE FOR CYCLE ON LOWERBOUNDPOINTER " + this.lowerBoundPointer);
        for(PhysicalWindow window: this.lowerBoundPointer){
            MinTopKEntry lowerBoundPointed = window.getLowerBoundPointer();
            System.out.println("WINDOW'S  " + window.getId() + " LowerBoundPointed: " + lowerBoundPointed);
            //add new record to superTopKList
            if(!isNewEntryCreated) {
                MinTopKEntry newEntry = new MinTopKEntry(movie.getId(), movie.getScore(), this.currentWindow.getId(),
                        this.currentWindow.getId() + (long) SIZE / HOPPING_SIZE - 1L);
                System.out.println("NEWENTRY " + newEntry);
                //insert newEntry in superTopKList
                insertNewEntry(newEntry);
                isNewEntryCreated = true;
                //case that topKCounter < K and new record score < lowerBound score --> new record become lowerBound
                if(lowerBoundPointed.getScore() >= movie.getScore() && window.getTopKCounter() < this.k){
                    window.setLowerBoundPointer(newEntry);
                    window.increaseTopKCounter(1);
                    this.physicalWindowsStore.put(window.getId(), window);
                }
            }
            int index = this.superTopKList.indexOf(lowerBoundPointed);
            if(index == -1){
                //lowerBoundPointed removed previously from the topKList, update lowerBound to the last element of topKList
                MinTopKEntry newLowerBound = this.superTopKList.get(this.superTopKList.size() - 1);
                window.setLowerBoundPointer(newLowerBound);
                this.physicalWindowsStore.put(window.getId(), window);
            } else if(lowerBoundPointed.getScore() < movie.getScore()){
                if(window.getTopKCounter() < this.k){
                    System.out.println("Increasing TopKCounter of " + window);
                    window.increaseTopKCounter(1);
                    this.physicalWindowsStore.put(window.getId(), window);
                } else {
                    System.out.println("Increasing starting window of " + lowerBoundPointed);
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
                    System.out.println("New WINDOW " + window.getId() + " LowerBoundPointed: " + newLowerBound);
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
            // if the element you are looking at is smaller than x,
            // go to the next element
            if (this.superTopKList.get(i).getScore() >= newEntry.getScore()) continue;
            // if the element equals x, return, because we don't add duplicates
            if (this.superTopKList.get(i).equals(newEntry)) return;
            // otherwise, we have found the location to add x
            this.superTopKList.add(i, newEntry);
            return;
        }
        if(size < this.k)
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
     * forward records as (key:windowId, value:movie.id)
     */
    private void forwardTopK(long windowId){
       List<MinTopKEntry> topK = this.superTopKList.subList(0,min(this.superTopKList.size(),this.k));
       topK.forEach(elem -> {
           System.out.println("FORWARDING KEY: " + windowId +" VALUE: " + elem.getId() );
           this.context.forward(windowId ,elem.getId());
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
                if(keyValue.key != -1)
                    this.lowerBoundPointer.add(Math.toIntExact(keyValue.key), keyValue.value);
                else
                    this.currentWindow = keyValue.value;
            }
            else
                this.lowerBoundPointer.add(keyValue.value);
        }
        lowerBoundPointerIterator.close();
    }

    private void saveDataStructures(){
        final Integer[] i = {0};
        this.superTopKList.forEach(elem -> {
            this.superTopKListStore.put(i[0], elem);
            i[0]++;
        });
        //save last entry of superTopKList
        this.superTopKListStore.put(-1, this.superTopKList.get(this.superTopKList.size() -1));
        this.lowerBoundPointer.forEach(elem -> {
            this.physicalWindowsStore.put(elem.getId(), elem);
        });
        //save current window
        this.physicalWindowsStore.put(-1L, this.currentWindow);
    }

    public void close() {
        // can access this.state
    }
}

