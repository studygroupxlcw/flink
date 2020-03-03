package study.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import study.flink.bean.Word;

public class WcFlatMap implements FlatMapFunction<String, Word> {

    @Override
    public void flatMap(String value, Collector<Word> out) throws Exception {
        String[] values = value.split(" ");
        out.collect(new Word("", values[0], Integer.parseInt(values[1])));
    }
}


