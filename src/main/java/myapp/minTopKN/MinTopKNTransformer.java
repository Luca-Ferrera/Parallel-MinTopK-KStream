package myapp.minTopKN;

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

public class MinTopKNTransformer implements Transformer<String, ScoredMovie, KeyValue<Long,MinTopKEntry>> {
    private KeyValueStore<Integer, MinTopKEntry> superTopKNListStore;
    private KeyValueStore<Long, PhysicalWindow> physicalWindowsStore;
    private final int k;
    private final int n;
    private final Boolean cleanDataStructure;
    private ProcessorContext context;
    private final int SIZE = 1200;
    private final int HOPPING_SIZE = 300;
    private ArrayList<MinTopKEntry> superTopKNList;
    private LinkedList<PhysicalWindow> lowerBoundPointer;
    private PhysicalWindow currentWindow;
    private PhysicalWindow lastWindow;
    private double minScore;

    public MinTopKNTransformer(int k, int n, String cleanDataStructure) {
        this.k = k;
        this.n = n;
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
        superTopKNListStore = (KeyValueStore<Integer, MinTopKEntry>) context.getStateStore("super-topk-list-store");
        //clear data structure
        if(this.cleanDataStructure){
            System.out.println("Cleaning Data Structures");
            physicalWindowsStore.all().forEachRemaining(elem -> physicalWindowsStore.delete(elem.key));
            superTopKNListStore.all().forEachRemaining(elem -> superTopKNListStore.delete(elem.key));
            return null;
        }
//        System.out.println("TRANSFORM KEY: " + key + " VALUE: " + value);
        setUpDataStructures();
        KeyValueIterator<Long, PhysicalWindow> windowsIterator = physicalWindowsStore.all();
        MinTopKEntry newEntry = null;
        //TODO: CHECK THIS PART
        if(!windowsIterator.hasNext()) {
            //initialize minScore as the score of the first record
            minScore = value.getScore();
            //create first window
//            System.out.println("EMPTY WINDOWS STORE");
            MinTopKEntry firstEntry = new MinTopKEntry(value.getId(), value.getScore(),
                    0L, + (long) SIZE / HOPPING_SIZE - 1L);
            this.superTopKNList.add(firstEntry);
            PhysicalWindow  startingWindow = new PhysicalWindow(0L, SIZE, HOPPING_SIZE, 1, 1 ,firstEntry);
            physicalWindowsStore.put(startingWindow.getId(), startingWindow);
            physicalWindowsStore.put(-1L, startingWindow);
            physicalWindowsStore.put(-2L, startingWindow);
            this.lowerBoundPointer.add(startingWindow);
            this.currentWindow = startingWindow;
            this.lastWindow = startingWindow;
            if(startingWindow.getActualRecords() == startingWindow.getHoppingSize())
                // case of HOPPING_SIZE == 1
                this.createNewWindow();
        } else {
            //maybe useless?
            windowsIterator = physicalWindowsStore.all();
            while (windowsIterator.hasNext()) {
                KeyValue<Long, PhysicalWindow> keyValue = windowsIterator.next();
                PhysicalWindow window = keyValue.value;
                // CheckNewActiveWindow (O i .t);  Algo 1 line 4
                if(window.getId() == this.lastWindow.getId() && window.getActualRecords() - window.getHoppingSize() == 1) {
                    // time to create new window
                    this.createNewWindow();
                }
                if(window.getId() == this.currentWindow.getId() && window.getActualRecords() - SIZE == 1) {
                    // current window ends
                    //TODO: receive changed objects
                    LinkedList<MinTopKEntry> changedObjects = new LinkedList<>();
                    this.updateChangedObjects(changedObjects);
                    this.forwardTopK(window.getId());
                    this.purgeExpiredWindow(window);
                }
                else {
                    //TODO: calculate the new score using minScore
                    MinTopKEntry entry = new MinTopKEntry(
                            value.getId(),
                            value.getScore(),
                            this.currentWindow.getId(),
                            this.currentWindow.getId() + (long) SIZE / HOPPING_SIZE
                    );
                    //updateMTK(O i); Algo 1 line 6
                    this.updateMTK(entry);
                }

            }
        }
        windowsIterator.close();
        saveDataStructures();
        return null;
    }

    private void createNewWindow() {
        //lower bound set to null since topK is not reached for this window
        PhysicalWindow newWindow = new PhysicalWindow(this.currentWindow.getId() + 1, SIZE, HOPPING_SIZE, 1, 1, null);
        this.physicalWindowsStore.put(newWindow.getId(), newWindow);
        this.physicalWindowsStore.put(-2L, newWindow);
        this.lastWindow = newWindow;
        this.lowerBoundPointer.add(newWindow);
    }

    private void updateChangedObjects(LinkedList<MinTopKEntry> changedObjects){
        changedObjects.forEach(this::updateMTK);
    }


    private void purgeExpiredWindow(PhysicalWindow window){
        // for first topK elements in superTopKNList:
        // - if current window == startingWindow increase by 1 the startingWindow
        // - if startingWindow > endingWindow remove the elem from superTopKNList
        Iterator<MinTopKEntry> superTopKIterator = this.superTopKNList.iterator();
        int i = 0;
        while(superTopKIterator.hasNext() && i < this.k) {
            MinTopKEntry entry = superTopKIterator.next();
            if(window.getId() == entry.getStartingWindow()) {
                entry.increaseStartingWindow(1L);
                i++;
            }
            if(entry.getStartingWindow() > entry.getEndingWindow()) {
                superTopKIterator.remove();
                this.superTopKNListStore.delete(i);
                //TODO implement
                //this.updateLBP();
            }
        }
        //Remove w exp from W act ; Algo 2 line 16
        this.physicalWindowsStore.delete(window.getId());
        this.lowerBoundPointer.remove(Math.toIntExact(window.getId()));
        //TODO: reset min.score? see annotation in PDF
    }

    private void updateLBP(MinTopKEntry entry){
        for(long i = entry.getStartingWindow(); i <= entry.getEndingWindow(); i++) {
            PhysicalWindow window = this.lowerBoundPointer.get(Math.toIntExact(i));
            MinTopKEntry lowerBound = window.getLowerBoundPointer();
            if(lowerBound == null) {
                window.increaseTopKCounter(1);
                if (window.getTopKCounter() == this.k + this.n) {
                    //TODO: implement
                    MinTopKEntry newLowerBound = this.generateLBP(window.getId());
                    window.setLowerBoundPointer(newLowerBound);
                }
            }
            else if(lowerBound.getScore() <= entry.getScore()) {
                //TODO: check if correct
                lowerBound.increaseStartingWindow(1);
                if(lowerBound.getStartingWindow() > lowerBound.getEndingWindow()) {
                    //Move w i .lbp by one position up in the MTK+N list; Algo 4 line 12
                    int index = this.superTopKNList.indexOf(lowerBound);
                    MinTopKEntry newLowerBound = this.superTopKNList.get(index - 1);
                    window.setLowerBoundPointer(newLowerBound);
                    this.physicalWindowsStore.put(window.getId(), window);
                    //Remove O w i .lbp from Super-MTK+N list; Algo 4 line 13
                    this.superTopKNList.remove(index);
                    this.superTopKNListStore.delete(index);
                }
            }

        }
    }

    private void updateMTK(MinTopKEntry entry){
        //update minScore
        this.minScore = Math.min(entry.getScore(), this.minScore);
        MinTopKEntry oldEntry = this.superTopKNList.stream().filter(elem -> elem.getId() == entry.getId()).findFirst().orElse(null);
        if(oldEntry != null) {
            //remove old object from superTopKNList
            this.superTopKNList.remove(oldEntry);
            //TODO: RefreshLBP(); Algo 3 line 7, see annotation in PDF
        }
        this.insertToMTK(entry);
    }

    private void insertToMTK(MinTopKEntry entry) {
        if(entry.getScore() < this.superTopKNList.get(this.superTopKNList.size() -1).getScore() && everyWindowHasTopKPlusN()){
            return;
        }
        //add O i to MTK+N list; Algo 3 line 25
        this.insertNewEntry(entry);
        //TODO: implement
        this.updateLBP(entry);
    }

    private MinTopKEntry generateLBP(long id){
        int i = 0;
        for (MinTopKEntry entry : this.superTopKNList) {
            if (entry.getEndingWindow() >= id) {
                i++;
            }
            if (i == this.k + this.n) {
                return entry;
            }
        }
        return null;
    }

    private void updateSuperTopK(ScoredMovie movie, MinTopKEntry topKEntry){
        if(movie.getScore() < this.superTopKNList.get(this.superTopKNList.size() -1).getScore() && everyWindowHasTopKPlusN()){
            return;
        }
        //add new record to superTopKNList
        MinTopKEntry newEntry;
        //this handle the case of the first HOPPING_SIZE records in the application
        //these records expires in SIZE / HOPPING_SIZE - 1 windows instead of SIZE / HOPPING_SIZE windows
        List<PhysicalWindow> physicalWindowList = this.lowerBoundPointer.stream().filter(window -> window.getId() == 0).collect(Collectors.toList());
        if(this.currentWindow.getId() == 0 && !physicalWindowList.isEmpty() && physicalWindowList.get(0).getActualRecords() <= HOPPING_SIZE){
            newEntry = new MinTopKEntry(movie.getId(), movie.getScore(), 0L,
                    (long) SIZE / HOPPING_SIZE - 1L);
        }else {
            //if newWindow is created I have already created the new entry for the superTopKNList, otherwise create it
            newEntry = topKEntry == null ? new MinTopKEntry(movie.getId(), movie.getScore(), this.currentWindow.getId(),
                    this.currentWindow.getId() + (long) SIZE / HOPPING_SIZE) : topKEntry;
        }
//        System.out.println("NEWENTRY " + newEntry);
        //insert newEntry in superTopKNList
        insertNewEntry(newEntry);
//        System.out.println("BEFORE FOR LOOP ON LOWERBOUNDPOINTER " + this.lowerBoundPointer);
        for(PhysicalWindow window: this.lowerBoundPointer){
            MinTopKEntry lowerBoundPointed = window.getLowerBoundPointer();
//            System.out.println("WINDOW'S  " + window.getId() + " LowerBoundPointed: " + lowerBoundPointed);
            //case that topKCounter < K and new record score < lowerBound score --> new record become lowerBound
            if(lowerBoundPointed.getScore() >= movie.getScore() && window.getTopKCounter() < this.k){
                window.setLowerBoundPointer(newEntry);
                window.increaseTopKCounter(1);
                this.physicalWindowsStore.put(window.getId(), window);
            }
            int index = this.superTopKNList.indexOf(lowerBoundPointed);
            if(index == -1){
                //lowerBoundPointed removed previously from the topKList, update lowerBound to the last element of topKList
                MinTopKEntry newLowerBound = this.superTopKNList.get(this.superTopKNList.size() - 1);
                window.setLowerBoundPointer(newLowerBound);
                this.physicalWindowsStore.put(window.getId(), window);
            } else if(lowerBoundPointed.getScore() < movie.getScore()){
                if(window.getTopKCounter() < this.k){
//                    System.out.println("Increasing TopKCounter of " + window);
                    window.increaseTopKCounter(1);
                    this.physicalWindowsStore.put(window.getId(), window);
                } else {
//                    System.out.println("Increasing starting window of " + lowerBoundPointed);
                    //increase starting window
                    lowerBoundPointed.increaseStartingWindow(1L);
                    window.setLowerBoundPointer(lowerBoundPointed);
                    if(lowerBoundPointed.getStartingWindow() > lowerBoundPointed.getEndingWindow()) {
                        //remove from superTopKNList
                        this.superTopKNList.remove(index);
                        this.superTopKNListStore.delete(index);
                    } else{
                        //update lowerBoundPointed into superTopKNList
                        this.superTopKNList.set(index, lowerBoundPointed);
                        this.superTopKNListStore.put(index, lowerBoundPointed);
                    }
                    //move lowerBoundPointer one position up in the superTopKNList
                    MinTopKEntry newLowerBound = this.superTopKNList.get(index - 1);
                    window.setLowerBoundPointer(newLowerBound);
                    this.physicalWindowsStore.put(window.getId(), window);
//                    System.out.println("New WINDOW " + window.getId() + " LowerBoundPointed: " + newLowerBound);
                }
            }
        }
        //Update the superTopKNList element accordingly to the updates in the lowerBoundPointer
        for (MinTopKEntry entry : this.superTopKNList) {
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
        int size = this.superTopKNList.size();
        for (int i = 0; i < size; i++) {
            MinTopKEntry elem = this.superTopKNList.get(i);
            // if the element score you are looking at is bigger than newEntry score, go to the next element
            if (elem.getScore() >= newEntry.getScore()) continue;
            // if the element equals newEntry, return, because we don't add duplicates
            if (elem.equals(newEntry)) return;
            // otherwise, we have found the location to add newEntry
            this.superTopKNList.add(i, newEntry);
            return;
        }
        //add newEntry at the end of superTopKNList
        this.superTopKNList.add(newEntry);
    }

    private boolean everyWindowHasTopKPlusN(){
        for(PhysicalWindow window: this.lowerBoundPointer){
            if(window.getTopKCounter() != this.k+this.n)
                return false;
        }
        return true;
    }

    /*
     * forward records as (key:windowId, value:minTopKEntry)
     */
    private void forwardTopK(long windowId){
        List<MinTopKEntry> topK = this.superTopKNList.subList(0,min(this.superTopKNList.size(),this.k));
        topK.forEach(elem -> {
//           System.out.println("FORWARDING KEY: " + windowId +" VALUE: " + elem);
            this.context.forward(windowId ,elem);
        });
    }

    private void setUpDataStructures(){
        this.superTopKNList = new ArrayList<MinTopKEntry>();
        KeyValueIterator<Integer, MinTopKEntry> superTopKIterator = this.superTopKNListStore.all();
        while(superTopKIterator.hasNext()) {
            KeyValue<Integer, MinTopKEntry> keyValue = superTopKIterator.next();
            if(keyValue.key <= this.superTopKNList.size()) {
                this.superTopKNList.add(keyValue.key, keyValue.value);
            }
            else
                this.superTopKNList.add(keyValue.value);
        }
        superTopKIterator.close();
        this.lowerBoundPointer = new LinkedList<>();
        KeyValueIterator<Long, PhysicalWindow> lowerBoundPointerIterator = this.physicalWindowsStore.all();
        while(lowerBoundPointerIterator.hasNext()){
            KeyValue<Long, PhysicalWindow> keyValue = lowerBoundPointerIterator.next();
            if(keyValue.key <= this.lowerBoundPointer.size()) {
                if(keyValue.key == -1) {
//                    System.out.println("loading currentWindow "+ keyValue.value);
                    this.currentWindow = keyValue.value;
                }
                else if(keyValue.key == -2){
//                    System.out.println("loading lastWindow "+ keyValue.value);
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
        //clear stateStore
        KeyValueIterator<Integer, MinTopKEntry> superTopKIterator = this.superTopKNListStore.all();
        while(superTopKIterator.hasNext()) {
            Integer keyValue = superTopKIterator.next().key;
            this.superTopKNListStore.delete(keyValue);
        }
        this.superTopKNList.forEach(elem -> {
//            System.out.println("Saving " + elem + "into superTopKStore");
            this.superTopKNListStore.put(i[0], elem);
            i[0]++;
        });
        this.lowerBoundPointer.forEach(elem -> {
            this.physicalWindowsStore.put(elem.getId(), elem);
        });
        //save current window
//        System.out.println("saving currentWindow" + this.currentWindow);
//        System.out.println("saving lastWindow" + this.lastWindow);
        this.physicalWindowsStore.put(-1L, this.currentWindow);
        this.physicalWindowsStore.put(-2L, this.lastWindow);
    }

    public void close() {
        // can access this.state
    }
}


