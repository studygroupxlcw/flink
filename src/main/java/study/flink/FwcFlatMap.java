package study.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import study.flink.bean.Word;

public class FwcFlatMap implements FlatMapFunction<String, Word> {
    @Override
    public void flatMap(String in, Collector<Word> out) throws Exception {
        String[] line = in.split("\\|\\|");
        String filename = line[0];
        String[] words = line[1].split(" ");
        for (String word : words) {
            out.collect(new Word(filename, word, 1));
        }
    }
}
