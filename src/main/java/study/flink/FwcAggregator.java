package study.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import study.flink.bean.Word;

public class FwcAggregator implements AggregateFunction<Word, Word, Word> {

    @Override
    public Word createAccumulator() {
        return new Word();
    }

    @Override
    public Word add(Word in, Word acc) {
        if (acc.word == null) {
            return new Word(in.filename, in.word, in.count);
        }

        if (acc.word.equals(acc.word)) {
            if (acc.filename.equals(acc.filename)) {
                acc.count = acc.count + in.count;
                return acc;
            } else {
                return new Word(in.filename, in.word, 1);
            }
        }

        return null;
    }

    @Override
    public Word getResult(Word accumulator) {
        return accumulator;
    }

    @Override
    public Word merge(Word a, Word b) {
        return new Word(a.filename, a.word, a.count + b.count);
    }
}
