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

public class DemoFlink {

    public  void wordCount2() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String input= "E:\\scala\\jwr\\flinkquickstartjava\\src\\main\\java\\sink-test.txt";
        DataSource<String> text = env.readTextFile(input);
       //String filename= text.getInputFormat().toString().split(" ")[1];
       // System.out.println("----"+text.getInputFormat().toString());
        text.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String s, Collector<WordCount> collector) throws Exception {
                String filename = s.split("\\|\\|")[0];
                System.out.println("filename:"+filename);
                String words= s.split("\\|\\|")[1].replaceAll("[\\pP\\p{Punct}]", "");
                System.out.println("words:"+words);
                String[] lines = words.split(" ");
                for (String line : lines) {
                    if (!line.isEmpty()){
                        collector.collect(new WordCount(filename+":"+line,1,System.currentTimeMillis()/1000));
                    }
                }
            }
        }).groupBy(0).sum(1).print();
        env.execute("stateful Window WordCount");


    }
    public  void wordCount1() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        // 设置flink重启策略
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 1000));
      //  DataStream<String> text = env.socketTextStream("149.248.60.109", 9009, "\n").filter(s -> s.split("\\|\\|").length!=2);
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");
       //时间
        DataStream marksSource = text.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor(Time.minutes(1)) {
            @Override
            public long extractTimestamp(Object o) {
                return System.currentTimeMillis()/1000;
            }
        });
     //   text.print();


        marksSource.flatMap(new FlatMapFunction<String, WordCount>() {
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
              .timeWindow(Time.minutes(1), Time.minutes(1))
               .aggregate(new newWcSumAggregator()).print();
//        inputText.returns(WordCount.class).keyBy("word")
//                .flatMap(new WordCountUtil())
//                .print();


//        SingleOutputStreamOperator<WordCount> results = inputText.returns(WordCount.class).keyBy("word")
//                .flatMap(new WordCountUtil())
//                .filter(wordCount -> wordCount.getTimestamp() + 60 >= System.currentTimeMillis() / 1000);
//        results.print();
//        new Timer().schedule(new TimerTask() {
//            @Override
//            public void run() {
//
//                try {
//                    Thread.sleep(10000);
//                    if (!results.toString().isEmpty()){
//                        System.out.println("---------------------"+System.currentTimeMillis()/1000);
//
//                        results.returns(WordCount.class).print();
//                        results.print();
//                    }
//
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//
//            }
//        },10000,10000);

//        DataStream results = inputText.keyBy(0).
//                window(TumblingEventTimeWindows.of(Time.minutes(1))).

               // sum(1);
               // aggregate(new newWcSumAggregator()).setParallelism(1);
        env.execute("stateful Window WordCount");

    }



    public static void main(String[] args) throws Exception {

        DemoFlink df = new DemoFlink();
        df.wordCount1();
    }
}

