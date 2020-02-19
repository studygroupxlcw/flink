package study.flink.bootstrap;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import study.flink.FwcAggregator;
import study.flink.FwcFlatMap;
import study.flink.bean.Word;

public class FilewordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取输入
        DataStreamSource<String> text = env.socketTextStream("192.168.1.210", 9999, "\n");

        // 根据单词分组统计
        DataStream<Word> wordCount = text.flatMap(new FwcFlatMap())
                .keyBy("word")
                .timeWindow(Time.seconds(600), Time.seconds(60))
                .aggregate(new FwcAggregator());
//                .sum("count");

        wordCount.print()
                .setParallelism(1);//使用一个并行度

        env.execute("streaming file word count");
    }
}
