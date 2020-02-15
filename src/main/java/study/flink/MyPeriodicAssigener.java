package study.flink;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import study.flink.bean.Word;

import javax.annotation.Nullable;

public class MyPeriodicAssigener implements AssignerWithPeriodicWatermarks<Word> {

    long maxTimeLag = 60 * 1000L;
    long currenttime = 0;
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currenttime - maxTimeLag);
    }

    @Override
    public long extractTimestamp(Word o, long l) {
        currenttime = Math.max(currenttime, System.currentTimeMillis());
        return System.currentTimeMillis();
    }
}
