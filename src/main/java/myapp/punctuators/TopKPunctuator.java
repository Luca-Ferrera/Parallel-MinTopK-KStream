package myapp.punctuators;

import myapp.avro.ScoredMovie;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.lang.Math.min;

public class TopKPunctuator implements Punctuator {

    private KeyValueStore<String, ValueAndTimestamp<ScoredMovie>> state1;
    private KeyValueStore<String, ScoredMovie> state2;
    private ProcessorContext context;
    private int k;
    private boolean isState2;

    public TopKPunctuator(KeyValueStore state1, ProcessorContext context, int k) {
        this.state1 = state1;
        this.context = context;
        this.k = k;
        this.isState2 = false;
    }
    public TopKPunctuator(KeyValueStore state1, KeyValueStore state2, ProcessorContext context, int k) {
        this.state1 = state1;
        this.state2 = state2;
        this.context = context;
        this.k = k;
        this.isState2 = true;
    }

    @Override
    public void punctuate(long l) {
        ArrayList<KeyValue<String, ScoredMovie>> scoreList = new ArrayList<>();
        this.state1.all().forEachRemaining((elem)-> {
            KeyValue<String,ScoredMovie> entry = new KeyValue<>(elem.key, elem.value.value());
            scoreList.add(entry);
        });
        Comparator<KeyValue<String, ScoredMovie>> compareByScore = Comparator.comparingDouble((KeyValue<String, ScoredMovie> o) -> o.value.getScore());
        Collections.sort(scoreList, compareByScore.reversed()
        );
        List<KeyValue<String, ScoredMovie>> topKList = scoreList.subList(0, min(this.k, scoreList.size()));
        //enter here if intermediate topKPhase
        if(this.isState2) {
            if(!this.state2.all().hasNext()) {
                final Integer[] i = {1};
                topKList.forEach(elem -> {
                    this.state2.put(i[0].toString(), elem.value);
                    context.forward(elem.key, elem.value);
                    i[0]++;
                });
            } else {
                final Integer[] position = {1};
                topKList.forEach(elem -> {
                    this.state2.all().forEachRemaining(stateElem -> {
                        // forward only if there are updates in the topKList
                        if (stateElem.key.equals(position[0].toString()) && stateElem.value.getId() != elem.value.getId()) {
                            this.state2.put(position[0].toString(), elem.value);
                            context.forward(elem.key, elem.value);
                        }
                    });
                    position[0]++;
                });
            }
        } else {
            // used by the centralized aggregator for final topK results
            topKList.forEach(elem -> context.forward(elem.key, elem.value));
        }
    }
}
