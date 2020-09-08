package myapp.distributedMinTopKN;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import myapp.avro.MinTopKEntry;
import myapp.avro.MovieIncome;
import myapp.avro.PhysicalWindow;
import myapp.avro.ScoredMovie;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.*;

import static java.lang.Integer.min;


public class DistributedMinTopKNTransformer implements Transformer<String, ScoredMovie, KeyValue<Long, MinTopKEntry>> {
    private KeyValueStore<Integer, MinTopKEntry> superTopKNListStore;
    private KeyValueStore<Long, PhysicalWindow> physicalWindowsStore;
    private final int k;
    private final int n;
    private final Boolean cleanDataStructure;
    private ProcessorContext context;
    private final int SIZE = 12;
    private final int HOPPING_SIZE = 3;
    private final int NUM_INSTANCES = 3;
    private final int LOCAL_SIZE = SIZE/NUM_INSTANCES;
    private final int LOCAL_HOPPING_SIZE = HOPPING_SIZE/NUM_INSTANCES;
    private ArrayList<MinTopKEntry> superTopKNList;
    private LinkedList<PhysicalWindow> lowerBoundPointer;
    private PhysicalWindow currentWindow;
    private PhysicalWindow lastWindow;
    private double minScore;

    public DistributedMinTopKNTransformer(int k, int n, String cleanDataStructure) {
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
        System.out.println("TRANSFORM KEY: " + key + " VALUE: " + value);
        setUpDataStructures();
        //TODO: CHECK THIS PART
        if(this.lowerBoundPointer.isEmpty()) {
            System.out.println("+++WINDOW ITERATOR EMPTY+++");
            //initialize minScore as the score of the first record
            this.minScore = value.getScore();
            //create first window
//            System.out.println("EMPTY WINDOWS STORE");
            MinTopKEntry firstEntry = new MinTopKEntry(value.getId(), value.getScore(),
                    0L, 0L);
            this.superTopKNList.add(firstEntry);
            // new window with null as LBP since actualRecords < K+N
            PhysicalWindow  startingWindow = new PhysicalWindow(0L, LOCAL_SIZE, LOCAL_HOPPING_SIZE, 1, 1 ,null);
            physicalWindowsStore.put(startingWindow.getId(), startingWindow);
            physicalWindowsStore.put(-1L, startingWindow);
            physicalWindowsStore.put(-2L, startingWindow);
            this.lowerBoundPointer.add(startingWindow);
            this.currentWindow = startingWindow;
            this.lastWindow = startingWindow;
            if(startingWindow.getActualRecords() == startingWindow.getHoppingSize())
                // case of LOCAL_HOPPING_SIZE == 1
                this.createNewWindow();
        } else {
            PhysicalWindow lastWindow = this.lowerBoundPointer.getLast();
            // CheckNewActiveWindow (O i .t);  Algo 1 line 4
            if(lastWindow.getActualRecords() - LOCAL_HOPPING_SIZE == 1) {
                // time to create new window
                System.out.println("+++CREATING NEW WINDOW+++");
                this.createNewWindow();
            }
            for(Iterator<PhysicalWindow> iterator = this.lowerBoundPointer.iterator(); iterator.hasNext();) {
//                System.out.println("LBP: " + lowerBoundPointer);
                PhysicalWindow window = iterator.next();
                int index = this.lowerBoundPointer.indexOf(window);
//                System.out.println("+++WINDOW+++ " + window);
                window.increaseActualRecords(1);
                // update StateStore
                physicalWindowsStore.put(window.getId(), window);
//                System.out.println("CURRENT WINDOW " + currentWindow);
                if(window.getId() == this.currentWindow.getId() && window.getActualRecords() - LOCAL_SIZE == 1) {
                    // current window ends
                    System.out.println("+++CURRENT WINDOW ENDS +++" + window);
                    LinkedList<MovieIncome> changedObjects = this.getUpdates();
//                    System.out.println("UPDATED OBJECTS " + changedObjects);
                    this.updateChangedObjects(changedObjects);
                    this.forwardTopK(window.getId());
                    this.purgeExpiredWindow(window);
                    iterator.remove();
                }
                else {
                    //TODO: calculate the new score using minScore
                    MinTopKEntry entry;
                    // first LOCAL_HOPPING_SIZE * (LOCAL_SIZE/LOCAL_HOPPING_SIZE) records are alive in less
                    // than LOCAL_SIZE/LOCAL_HOPPING_SIZE windows
                    if(this.currentWindow.getActualRecords() < LOCAL_HOPPING_SIZE * (LOCAL_SIZE/LOCAL_HOPPING_SIZE)) {
                        entry = new MinTopKEntry(
                                value.getId(),
                                value.getScore(),
                                this.currentWindow.getId(),
                                this.currentWindow.getId() + (this.currentWindow.getActualRecords()-1)/LOCAL_HOPPING_SIZE
                        );
                    } else {
                        entry = new MinTopKEntry(
                                value.getId(),
                                value.getScore(),
                                this.currentWindow.getId(),
                                this.currentWindow.getId() + (long) LOCAL_SIZE / LOCAL_HOPPING_SIZE
                        );
                    }
                    //updateMTK(O i); Algo 1 line 6
                    this.updateMTK(entry);
                }

            }
//            System.out.println("LAST WINDOW AFTER: " + this.lastWindow);
//            System.out.println("CURRENT WINDOW AFTER: " + this.currentWindow);
        }
        saveDataStructures();
        return null;
    }

    private void createNewWindow() {
        //lower bound set to null since topK is not reached for this window
        //TODO: set topKCounter to 0 ?
        PhysicalWindow newWindow = new PhysicalWindow(this.lastWindow.getId() + 1, LOCAL_SIZE, LOCAL_HOPPING_SIZE, 1, 1, null);
        this.physicalWindowsStore.put(newWindow.getId(), newWindow);
        this.physicalWindowsStore.put(-2L, newWindow);
        this.lastWindow = newWindow;
        this.lowerBoundPointer.add(newWindow);
    }

    private void updateChangedObjects(LinkedList<MovieIncome> updates){
        LinkedList<MinTopKEntry> changedObjects = new LinkedList<>();
        updates.forEach(update -> {
            MinTopKEntry entry= this.superTopKNList
                    .stream()
                    .filter(e -> e.getId() == update.getId())
                    .findFirst().orElse(null);
            //update score
            if(entry != null) {
                double newScore = entry.getScore() + 0.2 * update.getIncome();
                entry.setScore(newScore);
                changedObjects.add(entry);
            }
        });
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
                //save entry update in stateStore
                this.superTopKNListStore.put(i, entry);
                i++;
            }
            if(entry.getStartingWindow() > entry.getEndingWindow()) {
                superTopKIterator.remove();
                this.superTopKNListStore.delete(i);
                this.updateLBP(entry, window.getId());
            }
        }
        //Remove w exp from W act ; Algo 2 line 16
        this.physicalWindowsStore.delete(window.getId());
        //window is removed from lowerBoundPointer by the caller using iterator.remove()
        //update currentWindow
        this.currentWindow = this.physicalWindowsStore.get(window.getId() + 1L);
        this.physicalWindowsStore.put(-1L, this.currentWindow);
        //TODO: reset min.score? see annotation in PDF
    }

    private void updateLBP(MinTopKEntry entry, long expiredWindowId){
        for(PhysicalWindow window : this.lowerBoundPointer) {
//            System.out.println("ENTRY " + entry + " WINDOW " + window + " EXPIRED WINDOW ID " + expiredWindowId);
            if(window.getId() != expiredWindowId && entry.equals(window.getLowerBoundPointer())) {
                //update lowerBound -> set LB to null if window currentRecords < N+K
                MinTopKEntry newLowerBound = this.generateLBP(window.getId());
                window.setLowerBoundPointer(newLowerBound);
                this.physicalWindowsStore.put(window.getId(), window);
            }
        }
    }

    private void updateLBP(MinTopKEntry entry){
        for(long i = entry.getStartingWindow(); i <= entry.getEndingWindow() && i < this.lowerBoundPointer.size(); i++) {
            PhysicalWindow window = this.lowerBoundPointer.get(Math.toIntExact(i));
//            System.out.println("WINDOW UPDATELBP " + window);
            MinTopKEntry lowerBound = window.getLowerBoundPointer();
//            System.out.println("LOWER BOUND: "+ lowerBound + " WINDOW ID: " + i);
            if(lowerBound == null) {
//                System.out.println("INCREASING TOPKCOUNTER OF WINDOW: " + i);
                window.increaseTopKCounter(1);
                this.physicalWindowsStore.put(window.getId(), window);
                if (window.getTopKCounter() == this.k + this.n) {
                    MinTopKEntry newLowerBound = this.generateLBP(window.getId());
                    window.setLowerBoundPointer(newLowerBound);
                    this.physicalWindowsStore.put(window.getId(), window);
                }
            }
            else if(lowerBound.getScore() <= entry.getScore()) {
                //TODO: check if correct -> seems correct
                int index = this.superTopKNList.indexOf(lowerBound);
                // if index=-1 lowerBound in common with other window, it's already been removed from superTopKNList
                if(index == -1) {
                    lowerBound = this.generateLBP(window.getId());
                    // set new LBP if needed or set it to null
                    window.setLowerBoundPointer(lowerBound);
                    this.physicalWindowsStore.put(window.getId(), window);
                }
                else {
                    lowerBound.increaseStartingWindow(1);
//                    System.out.println("LOWER BOUND: " + lowerBound);
//                    System.out.println("SUPERTOPKN LIST: " + superTopKNList);
                    this.superTopKNList.set(index, lowerBound);
                    if(lowerBound.getStartingWindow() > lowerBound.getEndingWindow()) {
                        //Move w i .lbp by one position up in the MTK+N list; Algo 4 line 12
                        MinTopKEntry newLowerBound = this.superTopKNList.get(index - 1);
                        window.setLowerBoundPointer(newLowerBound);
                        this.physicalWindowsStore.put(window.getId(), window);
                        //Remove O w i .lbp from Super-MTK+N list; Algo 4 line 13
//                        System.out.println("REMOVING " + superTopKNList.get(index));
                        this.superTopKNList.remove(index);
                        this.superTopKNListStore.delete(index);
                    }
                }
            }

        }
    }

    private void updateMTK(MinTopKEntry entry){
//        System.out.println("ENTRY " + entry);
        //update minScore
//        System.out.println("CHECK IF EXIST " + superTopKNList);
        this.minScore = Math.min(entry.getScore(), this.minScore);
        MinTopKEntry oldEntry = this.superTopKNList.stream()
                .filter(elem -> elem.getId() == entry.getId() && elem.getScore() != entry.getScore())
                .findFirst().orElse(null);
        if(oldEntry != null) {
            //remove old object from superTopKNList
            this.superTopKNList.remove(oldEntry);
            // decrease topkCounter of windows where oldEntry belongs
            this.decreaseTopK(oldEntry);
            //TODO: RefreshLBP(); Algo 3 line 7, see annotation in PDF, maybe no needed
        }
        this.insertToMTK(entry, oldEntry);
    }

    private void decreaseTopK(MinTopKEntry entry) {
        //check all window where entry belongs
        for(long i = Math.max(entry.getEndingWindow() - LOCAL_SIZE/LOCAL_HOPPING_SIZE, 0); i <= entry.getEndingWindow() && i < this.lowerBoundPointer.size(); i++) {
            PhysicalWindow window = this.lowerBoundPointer.get(Math.toIntExact(i));
            //TODO: Decrease TopKCounter by adding -1 -> add decreaseTopKCounter method
            window.increaseTopKCounter(-1);
            // if window has less records than K + N remove LBP
            if(window.getTopKCounter() < this.n + this.k) {
                window.setLowerBoundPointer(null);
            }
        }
    }

    private void insertToMTK(MinTopKEntry entry, MinTopKEntry oldentry) {
        if(oldentry != null || entry.getScore() >= this.superTopKNList.get(this.superTopKNList.size() -1).getScore() || !everyWindowHasTopKPlusN()){
            //add O i to MTK+N list; Algo 3 line 25
            this.insertNewEntry(entry);
            this.updateLBP(entry);
        }
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

    private void insertNewEntry(MinTopKEntry newEntry){
        int size = this.superTopKNList.size();
        for (int i = 0; i < size; i++) {
            MinTopKEntry elem = this.superTopKNList.get(i);
            // if the element equals newEntry, return, because we don't add duplicates
            if (elem.equals(newEntry)) return;
            // if the element score you are looking at is bigger than newEntry score, go to the next element
            if (elem.getScore() >= newEntry.getScore()) continue;
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
            System.out.println("FORWARDING KEY: " + windowId +" VALUE: " + elem);
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
            else{
                this.superTopKNList.add(keyValue.value);
            }
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
                else if(keyValue.key == -2) {
//                    System.out.println("loading lastWindow "+ keyValue.value);
                    this.lastWindow = keyValue.value;
                }
                else {
//                    System.out.println("ADDING " + keyValue.value + " TO LBP");
                    this.lowerBoundPointer.add(Math.toIntExact(keyValue.key), keyValue.value);
                }
            }
            else {
                this.lowerBoundPointer.add(keyValue.value);
            }
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
//            System.out.println("SAVING " + elem);
            this.physicalWindowsStore.put(elem.getId(), elem);
        });
        //save current window
//        System.out.println("saving currentWindow" + this.currentWindow);
//        System.out.println("saving lastWindow" + this.lastWindow);
        this.physicalWindowsStore.put(-1L, this.currentWindow);
        this.physicalWindowsStore.put(-2L, this.lastWindow);
    }

    private LinkedList<MovieIncome> getUpdates() {
        KafkaConsumer<String, MovieIncome> consumer = setUpConsumer();
        consumer.subscribe(Arrays.asList("movie-updates"));
        LinkedList<MovieIncome> updates = new LinkedList<>();
        int noRecordsCount = 0;
        while (true) {
            ConsumerRecords<String, MovieIncome> records = consumer.poll(Duration.ofMillis(1000));
            //TODO: is it useful?
            if (records.count() == 0) {
                noRecordsCount++;
                // assuming n as maximum number of updates in the distributed database
                if (noRecordsCount > this.n) break;
                else continue;
            }
            for (ConsumerRecord<String, MovieIncome> record : records) {
                updates.add(record.value());
            }
            consumer.commitAsync();
        }
        consumer.close();
        return updates;
    }

    private KafkaConsumer<String, MovieIncome> setUpConsumer(){
        //TODO: set up env properties retrieval
        final String schemaRegistryUrl = "http://localhost:8081";
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "update-consumers");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        final SpecificAvroDeserializer<MovieIncome> movieIncomeDeserializer = new SpecificAvroDeserializer<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        movieIncomeDeserializer.configure(serdeConfig, false);
        KafkaConsumer<String, MovieIncome> consumer = new KafkaConsumer<>(props, Serdes.String().deserializer(), movieIncomeDeserializer);
        return consumer;
    }
    public void close() {
        // can access this.state
    }
}



