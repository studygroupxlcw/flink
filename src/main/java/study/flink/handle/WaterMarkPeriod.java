package study.flink.handle;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import study.flink.bean.WordBean;

import javax.annotation.Nullable;

public class WaterMarkPeriod implements AssignerWithPeriodicWatermarks<WordBean> {
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long currentSecond = System.currentTimeMillis() / 1000;
        long currentMinute = currentSecond - currentSecond % 60;
        return new Watermark(currentMinute * 1000);
    }

    @Override
    public long extractTimestamp(WordBean element, long previousElementTimestamp) {
        return 0;
    }
}
