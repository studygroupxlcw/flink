package jwr;

import org.apache.flink.api.common.functions.AggregateFunction;

public class newWcSumAggregator implements AggregateFunction<WordCount, WordCount, WordCount>  {

    @Override
    public WordCount createAccumulator() {
        return  new WordCount();
    }

    @Override
    public WordCount add(WordCount wordCount, WordCount wordCount2) {

           wordCount2.setTimestamp(wordCount.getTimestamp());
           wordCount2.setCount(wordCount.getCount());
           wordCount2.setWord(wordCount.getWord());


       // System.out.println(wordCount2.getWord()+"++"+wordCount2.getCount());
        return wordCount2;
    }

    @Override
    public WordCount getResult(WordCount wordCount) {
        return wordCount;
    }

    @Override
    public WordCount merge(WordCount wordCount, WordCount r) {
        wordCount.setTimestamp(wordCount.getTimestamp());
        wordCount.setCount(wordCount.getCount());

        return null;
    }
}