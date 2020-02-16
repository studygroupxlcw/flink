package jwr;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
//https://github.com/studygroupxlcw/flink.git
//https://github.com/jiaowr/flinkdemo.git
//git@github.com:studygroupxlcw/flink.git
public class DemoFlink {

    public  void wordCount2() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        // 设置flink重启策略
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 1000));
        //  DataStream<String> text = env.socketTextStream("149.248.60.109", 9009, "\n");
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");
        text.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String s, Collector<WordCount> collector) throws Exception {
                if (s.split("\\|\\|").length==2){
                    String filename = s.split("\\|\\|")[0];
                    String words= s.split("\\|\\|")[1].replaceAll("[\\pP\\p{Punct}]", "");
                    String[] lines = words.split(" ");
                    long captureTime = System.currentTimeMillis()/1000;
                    for (String line : lines) {
                        if (!line.isEmpty()){
                            collector.collect(new WordCount(filename+":"+line,1,captureTime));
                        }
                    }
                }
            }
        })
                .returns(WordCount.class).keyBy("word")
                .flatMap(new WordCountUtil()).returns(WordCount.class)
                .keyBy("word")
               // .window(TumblingEventTimeWindows.of(Time.hours(1)))
               // .trigger(new CustomTrigger(10, 1 * 60 * 1000L))
                .sum("count").print();
        env.execute("stateful tigger WordCount");

    }
    public  void wordCount1() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        // 设置flink重启策略
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 1000));
      //  DataStream<String> text = env.socketTextStream("149.248.60.109", 9009, "\n");
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");
        text.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String s, Collector<WordCount> collector) throws Exception {
                if (s.split("\\|\\|").length==2){
                    String filename = s.split("\\|\\|")[0];
                    String words= s.split("\\|\\|")[1].replaceAll("[\\pP\\p{Punct}]", "");
                    String[] lines = words.split(" ");
                    long captureTime = System.currentTimeMillis()/1000;
                    for (String line : lines) {
                        if (!line.isEmpty()){
                            collector.collect(new WordCount(filename+":"+line,1,captureTime));
                        }
                    }
                }
            }
       })
                .assignTimestampsAndWatermarks(new WatermarkGenerate())
                   .returns(WordCount.class).keyBy("word")
                .flatMap(new WordCountUtil()).returns(WordCount.class)
                .keyBy("word")
             .timeWindow(Time.minutes(10), Time.minutes(1))
                .aggregate(new newWcSumAggregator())
                .filter(wordCount->wordCount.getTimestamp() + 60 >= System.currentTimeMillis() / 1000)
                .print();
        env.execute("stateful Window WordCount");

    }



    public static void main(String[] args) throws Exception {

        DemoFlink df = new DemoFlink();
        //使用windows的方法
       // df.wordCount1();
        //非windows的方法 定时触发
        df.wordCount2();
    }
}

