package myapp;

import myapp.avro.ScoredMovie;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class MyPunctuator implements Punctuator {

    private KeyValueStore<String, ValueAndTimestamp<ScoredMovie>> state;
    private ProcessorContext context;

    public MyPunctuator(KeyValueStore state, ProcessorContext context) {
        this.state = state;
        this.context = context;
    }

    @Override
    public void punctuate(long l) {
        ArrayList<KeyValue<String, ScoredMovie>> scoreList = new ArrayList<>();
        this.state.all().forEachRemaining((elem)-> {
            KeyValue<String,ScoredMovie> entry = new KeyValue<String, ScoredMovie>(elem.key, elem.value.value());
            scoreList.add(entry);
        });
        Comparator<KeyValue<String, ScoredMovie>> compareByScore = Comparator.comparingDouble((KeyValue<String, ScoredMovie> o) -> o.value.getScore());
        Collections.sort(scoreList, compareByScore.reversed()
        );
        scoreList.forEach(elem -> context.forward(elem.key, elem.value));
    }
}
