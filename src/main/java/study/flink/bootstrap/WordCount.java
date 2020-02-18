package study.flink.bootstrap;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import study.flink.MessageFlatMap;
import study.flink.ResultFlatMap;
import study.flink.WatermarkGenerate;
import study.flink.WordAggregate;
import study.flink.bean.Word;

public class WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("149.248.60.109", 9009, "\n")
                .flatMap(new MessageFlatMap())
                .assignTimestampsAndWatermarks(new WatermarkGenerate())
                .keyBy((KeySelector<Word, Integer>) value -> Math.abs(value.value.hashCode() % 256))
                .timeWindow(Time.minutes(10), Time.minutes(1))
                .aggregate(new WordAggregate())
                .flatMap(new ResultFlatMap())
                .print();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.execute("test");
    }

}
