package study.flink.bootstrap;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import study.flink.CountAgg;
import study.flink.WcFlatMap;
import study.flink.bean.Word;

public class CountTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("192.168.1.210", 9999, "\n");

        text.flatMap(new WcFlatMap())
                .keyBy("word")
                .countWindow(4, 1)
//                .reduce((ReduceFunction<Word>) (value1, value2) -> new Word("", value1.word, value1.count + value2.count))
                .aggregate(new CountAgg())
                .print().setParallelism(1);


        env.execute("count test stream");
    }
}
