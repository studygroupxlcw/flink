package jwr;

import org.apache.flink.api.common.functions.AggregateFunction;

public class newWcSumAggregator implements AggregateFunction<WordCount, WordCount, WordCount>  {

    @Override
    public WordCount createAccumulator() {
        return  new WordCount();
    }

    @Override
    public WordCount add(WordCount wordCount, WordCount wordCount2) {
        wordCount.setTimestamp(wordCount.getTimestamp());
        wordCount.setCount(wordCount.getCount());
        return wordCount;
    }

    @Override
    public WordCount getResult(WordCount wordCount) {
        return wordCount;
    }

    @Override
    public WordCount merge(WordCount wordCount, WordCount acc1) {
        wordCount.setTimestamp(wordCount.getTimestamp());
        wordCount.setCount(wordCount.getCount());

        return wordCount;
    }
}
