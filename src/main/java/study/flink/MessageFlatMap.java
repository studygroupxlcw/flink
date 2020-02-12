package study.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import study.flink.bean.Word;

public class MessageFlatMap implements FlatMapFunction<String, Word> {
    @Override
    public void flatMap(String value, Collector<Word> out) throws Exception {
        String[] fields = value.split("\\|\\|");
        String fileName = fields[0];
        String[] words = fields[1].split(" ");
        long captureTime = System.currentTimeMillis();
        for (String w : words) {
            Word word = new Word();
            word.captureTime = captureTime;
            word.value = w.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
            word.fileName = fileName;
            word.cnt = 1;
            out.collect(word);
        }
    }
}
