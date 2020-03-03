package study.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import study.flink.bean.Word;

import java.util.Set;

public class ResultFlatMap implements FlatMapFunction<Set<Word>, String> {
    @Override
    public void flatMap(Set<Word> value, Collector<String> collector) throws Exception {
        value.forEach(val -> {
            collector.collect(val.toString());
        });
    }
}
