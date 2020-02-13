package study.flink.bootstrap;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import study.flink.bean.WordBean;
import study.flink.handle.LineFlatMap;
import study.flink.handle.WordAggregate;


public class QuickStart {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        setConfig(env);
        env.socketTextStream("149.248.60.109", 9009,"\n")
                .flatMap(new LineFlatMap()).name("line flapMap").uid("line flapMap")
                .keyBy(WordBean::getWordValue)
                .timeWindow(Time.minutes(10),Time.minutes(1))
                .aggregate(new WordAggregate()).name("arregate").uid("arregate")
                .print();
        env.execute("word test window");
    }

    public static void setConfig(StreamExecutionEnvironment env) {
        env.enableCheckpointing(1000);
        // advanced options:

// set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

// checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);

// allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

// enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

// allow job recovery fallback to checkpoint when there is a more recent savepoint
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
    }

}
