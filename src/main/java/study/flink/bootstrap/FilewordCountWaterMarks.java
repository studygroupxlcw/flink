package study.flink.bootstrap;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import study.flink.FwcFlatMap;
import study.flink.MyPeriodicAssigener;
import study.flink.bean.Word;

public class FilewordCountWaterMarks {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStreamSource<String> text = env.socketTextStream("192.168.1.210", 9999, "\n");

        DataStream<Word> wordCount = text.flatMap(new FwcFlatMap())
                .assignTimestampsAndWatermarks(new MyPeriodicAssigener())
                .keyBy("filename", "word")
                .sum("count");

        wordCount.print().setParallelism(1);

        env.execute("start wc with watermarks");

    }
}
