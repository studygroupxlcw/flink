package study.flink.bootstrap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import study.flink.MessageFlatMap;
import study.flink.ResultFlatMap;
import study.flink.WatermarkGenerate;
import study.flink.WordAggregate;
import study.flink.bean.Word;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("149.248.60.109", 9009, "\n")
                .flatMap(new MessageFlatMap())
                .assignTimestampsAndWatermarks(new WatermarkGenerate())
                .keyBy((KeySelector<Word, String>) value -> value.value)
                .timeWindow(Time.minutes(10), Time.minutes(1))
                .aggregate(new WordAggregate())
                .flatMap(new ResultFlatMap())
                .print();
        env.execute("test");
    }

}
