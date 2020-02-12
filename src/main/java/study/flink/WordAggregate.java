package study.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import study.flink.bean.Word;

import java.util.HashMap;
import java.util.Map;

public class WordAggregate implements AggregateFunction<Word, Map<String, Word>, Map<String, Word>> {
    @Override
    public Map<String, Word> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<String, Word> add(Word value, Map<String, Word> accumulator) {
        if (accumulator.containsKey(value.value)) {
            Word oldWord = accumulator.get(value.value);
            // 该单词在新的文本中出现则清除原来的统计信息
            if (!oldWord.fileName.equals(value.fileName) && value.captureTime > oldWord.captureTime) {
                accumulator.put(value.value, value);
                return accumulator;
            }
            if (oldWord.fileName.equals(value.fileName) && value.captureTime >= oldWord.captureTime) {
                oldWord.cnt = oldWord.cnt + value.cnt;
                oldWord.captureTime = value.captureTime;
            }
        } else {
            accumulator.put(value.value, value);
        }
        return accumulator;
    }

    @Override
    public Map<String, Word> getResult(Map<String, Word> accumulator) {
        return accumulator;
    }

    @Override
    public Map<String, Word> merge(Map<String, Word> a, Map<String, Word> b) {
        return null;
    }
}
