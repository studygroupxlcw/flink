package study.flink.bootstrap;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import study.flink.bean.WordBean;
import study.flink.handle.LineFlatMap;
import study.flink.handle.WaterMarkPeriod;
import study.flink.handle.ProcessFunctionHandle;

public class NoWindowStart extends QuickStart{
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        setConfig(env);

        env.socketTextStream("149.248.60.109", 9009,"\n")
                .flatMap(new LineFlatMap()).name("line flapMap").uid("line flapMap")
                .assignTimestampsAndWatermarks(new WaterMarkPeriod())
                .keyBy(WordBean::getWordValue)
                .process(new ProcessFunctionHandle())
                .print();
        env.execute("word test no window");
    }
}
