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

    private KeyValueStore<String, ValueAndTimestamp<ScoredMovie>> state;
    private ProcessorContext context;
    private int k;

    public TopKPunctuator(KeyValueStore state, ProcessorContext context, int k) {
        this.state = state;
        this.context = context;
        this.k = k;
    }

    @Override
    public void punctuate(long l) {
        ArrayList<KeyValue<String, ScoredMovie>> scoreList = new ArrayList<>();
        this.state.all().forEachRemaining((elem)-> {
            KeyValue<String,ScoredMovie> entry = new KeyValue<>(elem.key, elem.value.value());
            scoreList.add(entry);
        });
        Comparator<KeyValue<String, ScoredMovie>> compareByScore = Comparator.comparingDouble((KeyValue<String, ScoredMovie> o) -> o.value.getScore());
        Collections.sort(scoreList, compareByScore.reversed()
        );
        List<KeyValue<String, ScoredMovie>> topKList = scoreList.subList(0, min(this.k, scoreList.size()));
        topKList.forEach(elem -> context.forward(elem.key, elem.value));
    }
}
