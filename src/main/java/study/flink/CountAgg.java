package study.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import study.flink.bean.Word;

//public class CountAgg implements AggregateFunction<Word, Integer, Integer> {
//    @Override
//    public Integer createAccumulator() {
//        return Integer.valueOf(0);
//    }
//
//    @Override
//    public Integer add(Word value, Integer accumulator) {
//        return accumulator+value.count;
//    }
//
//    @Override
//    public Integer getResult(Integer accumulator) {
//        return accumulator;
//    }
//
//    @Override
//    public Integer merge(Integer a, Integer b) {
//        return a+b;
//    }
//}


public class CountAgg implements AggregateFunction<Word, Word, Word> {
    @Override
    public Word createAccumulator() {
        return new Word("", null, 0);
    }

    @Override
    public Word add(Word value, Word accumulator) {
        return new Word("", value.word, value.count+accumulator.count);
    }

    @Override
    public Word getResult(Word accumulator) {
        return accumulator;
    }

    @Override
    public Word merge(Word a, Word b) {
        return new Word("", a.word, a.count+b.count);
    }
}