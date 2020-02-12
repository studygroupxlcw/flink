package study.flink;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import study.flink.bean.Word;

import javax.annotation.Nullable;

public class WatermarkGenerate implements AssignerWithPeriodicWatermarks<Word> {
    private long maxCaptureTime = 0;
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxCaptureTime);
    }

    @Override
    public long extractTimestamp(Word element, long previousElementTimestamp) {
        if (maxCaptureTime < element.captureTime) {
            maxCaptureTime = element.captureTime;
        }
        return element.captureTime;
    }
}
