package myapp.distributedMaterializeScoreSort.PhysicalWindow;

import myapp.avro.ScoredMovie;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class CentralizedAggregatedSortTransformer implements Transformer<Long, ScoredMovie,  KeyValue<Long, ScoredMovie>> {

    private final Boolean cleanDataStructure;
    private KeyValueStore<Long, ArrayList<ScoredMovie>> windowedMoviesState;
    private ProcessorContext context;
    private int INSTANCE_NUMBER = 6;
    private int k;

    public CentralizedAggregatedSortTransformer(String cleanDataStructure, int k) {
        this.cleanDataStructure = cleanDataStructure.equals("clean");
        this.k = k;
    }

    public void init(ProcessorContext context) {
        this.context = context;
    }

    public KeyValue<Long,ScoredMovie> transform(Long key, ScoredMovie value) {
        this.windowedMoviesState = (KeyValueStore<Long, ArrayList<ScoredMovie>>) context.getStateStore("windowed-movies-store");
        if(this.cleanDataStructure){
            this.windowedMoviesState.all().forEachRemaining(elem -> windowedMoviesState.delete(elem.key));
            System.out.println("CLEANED");
            return null;
        }
//        System.out.println("The offset of the record " + key + " we just read is: " + this.context.offset());
        // insert new record in the store based on its key (windowID)
        this.windowedMoviesState.putIfAbsent(key, new ArrayList<ScoredMovie>());
        ArrayList<ScoredMovie> windowArray = this.windowedMoviesState.get(key);
        windowArray.add(value);
        this.windowedMoviesState.put(key, windowArray);
        //check if received all records from window with ID=key -> forward all records
        if(windowArray.size() == k * INSTANCE_NUMBER){
            Comparator<ScoredMovie> compareByScore = Comparator.comparingDouble(ScoredMovie::getScore);
            Collections.sort(windowArray, compareByScore.reversed());
            // remove the list associated to windowID=key because from now on it'll be useless
            this.windowedMoviesState.delete(key);
            // forward topK records for the windowID=key
            List<ScoredMovie> subList = windowArray.subList(0,k);
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

