package study.flink.handle;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import study.flink.bean.WordBean;

import java.lang.reflect.Array;
import java.util.Arrays;

public class LineFlatMap implements FlatMapFunction<String, WordBean> {
    @Override
    public void flatMap(String value, Collector<WordBean> out) throws Exception {
        String[] valueArr = value.split("\\|\\|");
        String fileName = valueArr[0];
        Arrays.stream(valueArr[1].split(" ")).map(it -> it.replaceAll("[^a-zA-Z0-9]", "")).forEach(it -> out.collect(new WordBean(fileName, it, 1)));
    }
}
