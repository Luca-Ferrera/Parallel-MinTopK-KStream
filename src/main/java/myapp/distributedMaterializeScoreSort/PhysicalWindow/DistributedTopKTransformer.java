package myapp.distributedMaterializeScoreSort.PhysicalWindow;

import myapp.avro.ScoredMovie;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.*;

public class DistributedTopKTransformer implements Transformer<String, ScoredMovie, KeyValue<Long,ScoredMovie>> {
    private KeyValueStore<Long, ArrayList<ScoredMovie>> windowedMoviesState;
    private KeyValueStore<Integer, Integer> countState;
    private String storeName1;
    private String storeName2;
    private ProcessorContext context;
    private final int SIZE = 3600;
    private final int HOPPING_SIZE = 300;
    private final int NUM_INSTANCES = 6;
    private final int LOCAL_SIZE = SIZE/NUM_INSTANCES;
    private final int LOCAL_HOPPING_SIZE = HOPPING_SIZE/NUM_INSTANCES;
    private final Boolean cleanDataStructure;
    private final int k;


    public DistributedTopKTransformer(String storeName1, String storeName2, String cleanDataStructure, int k) {
        this.cleanDataStructure = cleanDataStructure.equals("clean");
        this.storeName1 = storeName1;
        this.storeName2 = storeName2;
        this.k = k;
    }

    public void init(ProcessorContext context) {
        this.context = context;
        this.windowedMoviesState = (KeyValueStore<Long, ArrayList<ScoredMovie>> ) context.getStateStore(this.storeName1);
        this.countState = (KeyValueStore<Integer, Integer>) context.getStateStore(this.storeName2);
    }

    public  KeyValue<Long,ScoredMovie> transform(String key, ScoredMovie value) {
        if(this.cleanDataStructure){
            this.windowedMoviesState.all().forEachRemaining(elem -> windowedMoviesState.delete(elem.key));
            this.countState.delete(-1);
            System.out.println("CLEANED");
            return null;
        }
        int recordCount = this.countState.all().hasNext() ? this.countState.get(-1) : 1;
        long windowID = (recordCount - 1) / LOCAL_HOPPING_SIZE;
        this.windowedMoviesState.putIfAbsent(windowID, new ArrayList<ScoredMovie>());
        LinkedList<Long> windowIDList = new LinkedList<>();
        this.windowedMoviesState.all().forEachRemaining((elem) -> windowIDList.add(elem.key));

        ArrayList<ScoredMovie> windowArray;
        long windowIdToForward = windowIDList.getFirst();
        for(long id : windowIDList){
            if(recordCount < LOCAL_SIZE || recordCount % LOCAL_HOPPING_SIZE != 1 || id != windowIdToForward ) {
                windowArray = this.windowedMoviesState.get(id);
                windowArray.add(value);
                this.windowedMoviesState.put(id, windowArray);
            }
        }
        if(recordCount >= LOCAL_SIZE && recordCount % LOCAL_HOPPING_SIZE == 1) {
            windowArray = this.windowedMoviesState.get(windowIdToForward);
            Comparator<ScoredMovie> compareByScore = Comparator.comparingDouble(ScoredMovie::getScore);
            Collections.sort(windowArray, compareByScore.reversed());
            List<ScoredMovie> subList = windowArray.subList(0,k);
            subList.forEach(elem -> {
                context.forward(windowIdToForward, elem);
            });
        }
        if(windowIDList.size() > LOCAL_SIZE / LOCAL_HOPPING_SIZE) {
            //remove old window
            long item = windowIDList.removeFirst();
            this.windowedMoviesState.delete(item);
        }

        recordCount ++;
        this.countState.put(-1, recordCount);
        return null;
    }

    public void close() {
        // can access this.state
    }
}

