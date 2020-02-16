package jwr;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class WordCountUtil extends RichFlatMapFunction<WordCount, WordCount> {
    private transient ValueState<WordCountState> countState;
    @Override
    public void flatMap(WordCount wordCount, Collector<WordCount> collector) throws Exception {
        WordCountState lastState = countState.value();
        if (lastState == null) {
            // 初始化state
            lastState = new WordCountState(wordCount.getWord(), wordCount.getCount(),wordCount.getTimestamp());
            // 进来的数据原样返回
            collector.collect(wordCount);
            // 更新state
            countState.update(lastState);
        } else {
            // count 累加
            lastState.setCount(lastState.getCount() + wordCount.getCount());
            lastState.setTimestamp(wordCount.getTimestamp());
            collector.collect(new WordCount(wordCount.getWord(), lastState.getCount(),wordCount.getTimestamp()));
            // 更新state
            countState.update(lastState);
        }

    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<WordCountState> descriptor =
                new ValueStateDescriptor<>(
                        "countState",
                        TypeInformation.of(new TypeHint<WordCountState>() {
                        }));
        countState = getRuntimeContext().getState(descriptor);
    }

}
