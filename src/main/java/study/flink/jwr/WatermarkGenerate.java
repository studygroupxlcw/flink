package jwr;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class WatermarkGenerate implements AssignerWithPeriodicWatermarks<WordCount> {
    private long maxCaptureTime = 0;
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
         return new Watermark(maxCaptureTime);
    }

    @Override
    public long extractTimestamp(WordCount wordCount, long l) {
        if (maxCaptureTime < wordCount.getTimestamp()) {

            maxCaptureTime = wordCount.getTimestamp();
        }
       // System.out.println("time:"+wordCount.getTimestamp());
        return wordCount.getTimestamp();
    }
}

