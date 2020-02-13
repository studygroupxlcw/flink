package study.flink.handle;

import org.apache.flink.api.common.functions.AggregateFunction;
import study.flink.bean.WordBean;

public class WordAggregate implements AggregateFunction<WordBean, WordBean, String> {
    @Override
    public WordBean createAccumulator() {
        return new WordBean(null, null, 0);
    }

    @Override
    public WordBean add(WordBean value, WordBean accumulator) {
        if (0 == accumulator.getCount()) {
            accumulator.setFileName(value.getFileName());
            accumulator.setWordValue(value.getWordValue());
            accumulator.addCount(value.getCount());
        } else if (!value.getFileName().equals(accumulator.getFileName())) {
            accumulator.setFileName(value.getFileName());
            accumulator.setWordValue(value.getWordValue());
            accumulator.setCount(1);
        } else {
            accumulator.addCount(value.getCount());
        }
        return accumulator;
    }

    @Override
    public String getResult(WordBean accumulator) {
        return accumulator.getFileName() + ":" + accumulator.getWordValue() + ":" + accumulator.getCount();
    }

    @Override
    public WordBean merge(WordBean a, WordBean b) {
        return null;
    }
}
