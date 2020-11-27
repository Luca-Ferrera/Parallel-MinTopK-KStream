package myapp.materializeScoreSort.PhysicalWindow;

import myapp.avro.ScoredMovie;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.*;


public class MSSTopKTrasformer implements Transformer<String, ScoredMovie, KeyValue<Long, ScoredMovie>> {

    private final Boolean cleanDataStructure;
    private KeyValueStore<Long, ArrayList<ScoredMovie>> windowedMoviesState;
    private String storeName1;
    private String storeName2;
    private KeyValueStore<Integer, Integer> countState;
    private ProcessorContext context;
    private final int SIZE = 3600;
    private final int HOPPING_SIZE = 300;
    private final int k;

    public MSSTopKTrasformer(String storeName1, String storeName2, String cleanDataStructure, int k) {
        this.cleanDataStructure = cleanDataStructure.equals("clean");
        this.storeName1 = storeName1;
        this.storeName2 = storeName2;
        this.k = k;
    }

    public void init(ProcessorContext context) {
        this.context = context;
        this.windowedMoviesState = (KeyValueStore<Long, ArrayList<ScoredMovie>>) context.getStateStore(this.storeName1);
        this.countState = (KeyValueStore<Integer, Integer>) context.getStateStore(this.storeName2);
    }

    public KeyValue<Long,ScoredMovie> transform(String key, ScoredMovie value) {
        if(this.cleanDataStructure){
            this.windowedMoviesState.all().forEachRemaining(elem -> windowedMoviesState.delete(elem.key));
            this.countState.delete(-1);
            System.out.println("CLEANED");
            return null;
        }
        int recordCount = this.countState.all().hasNext() ? this.countState.get(-1) : 1;
        long windowID = (recordCount - 1) / HOPPING_SIZE;
        this.windowedMoviesState.putIfAbsent(windowID, new ArrayList<ScoredMovie>());
        LinkedList<Long> windowIDList = new LinkedList<>();
        this.windowedMoviesState.all().forEachRemaining((elem) -> windowIDList.add(elem.key));

        ArrayList<ScoredMovie> windowArray;
        long windowIdToForward = windowIDList.getFirst();
        for(long id : windowIDList){
            if(recordCount < SIZE || recordCount % HOPPING_SIZE != 1 || id != windowIdToForward ){
                windowArray = this.windowedMoviesState.get(id);
                windowArray.add(value);
                this.windowedMoviesState.put(id, windowArray);
            }
        }
        if(recordCount%1000 == 0 ) {
            System.out.println("+++ RECORD COUNT " + recordCount);
        }
        if(recordCount >= SIZE && recordCount % HOPPING_SIZE == 1) {
            windowArray = this.windowedMoviesState.get(windowIdToForward);
            Comparator<ScoredMovie> compareByScore = Comparator.comparingDouble(ScoredMovie::getScore);
            Collections.sort(windowArray, compareByScore.reversed());
            List<ScoredMovie> subList = windowArray.subList(0,k);
            subList.forEach(elem -> {
                context.forward(windowIdToForward, elem);
            });
        }
        if(windowIDList.size() > SIZE / HOPPING_SIZE) {
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

