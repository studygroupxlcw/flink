package jwr;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class WordCountUtil extends RichFlatMapFunction<WordCount,Map<String,WordCount>> {
    private transient ValueState<Map<String,WordCount>> countState;
//    @Override
//    public void open(Configuration config) {
//        ValueStateDescriptor<WordCountState> descriptor =
//                new ValueStateDescriptor<>(
//                        "countState",
//                        TypeInformation.of(new TypeHint<WordCountState>() {
//                        }));
//        countState = getRuntimeContext().getState(descriptor);
//    }

    @Override
    public void flatMap(WordCount wordCount, Collector<Map<String, WordCount>> collector) throws Exception {
        Map<String, WordCount> lastState  =countState.value();
        if (lastState.containsKey(wordCount.getWord())){
           WordCount oldstsate= lastState.get(wordCount.getWord());
           if (oldstsate.getFilename().equals(wordCount.getFilename())&&oldstsate.getTimestamp()<wordCount.getTimestamp()){
              wordCount.setCount(wordCount.getCount()+oldstsate.getCount());
               lastState.put(wordCount.getWord(),wordCount);
           }if (!oldstsate.getFilename().equals(wordCount.getFilename())){
              lastState.put(wordCount.getWord(),wordCount);
           }
        }else {
            lastState.put(wordCount.getWord(),wordCount);
            countState.update(lastState);
            collector.collect(lastState);
        }


    }
}
