package study.flink.handle;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import study.flink.bean.WordBean;

public class ProcessFunctionHandle extends KeyedProcessFunction<String,WordBean, String> {
    private transient ValueState<WordBean> result;

    @Override
    public void processElement(WordBean value, Context ctx, Collector<String> out) throws Exception {
        if (null != result.value()) {
            if (!result.value().getFileName().equals(value.getFileName())) {
                result.update(value);
            } else {
                result.value().addCount(value.getCount());
                result.value().setCurrentTime(value.getCurrentTime());
            }
        } else {
            result.update(value);
        }
        ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 60_000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        System.out.println(timestamp+":"+result.value().getCurrentTime());
        out.collect(result.value().getFileName() + ":" + result.value().getWordValue() + ":" + result.value().getCount());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<WordBean> valueStateDescriptor = new ValueStateDescriptor<>("wordValueStat", TypeInformation.of(WordBean.class));
        result = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
