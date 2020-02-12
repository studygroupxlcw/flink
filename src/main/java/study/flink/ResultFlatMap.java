package study.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import study.flink.bean.Word;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class ResultFlatMap implements FlatMapFunction<Map<String, Word>, String> {

    private SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
    @Override
    public void flatMap(Map<String, Word> value, Collector<String> out) throws Exception {
        value.forEach(
                (key, word) -> {
                    out.collect(word.fileName + ":" + word.value + ":" + word.cnt + ":" + format.format(new Date(word.captureTime)));
                }
        );
    }
}
