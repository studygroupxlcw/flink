package jwr;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class ProcessTimeTrigger<W extends Window> extends Trigger<Object, W> {
    private final long interval;
    public ProcessTimeTrigger(long interval) {
        this.interval = interval;
    }
    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("ptcls-time", new ReduceMin(), LongSerializer.INSTANCE);
    @Override
    public TriggerResult onElement(Object o, long l, W w, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);

        long timestamp = ctx.getCurrentProcessingTime();

        if (fireTimestamp.get() == null) {
            long start = timestamp - (timestamp % interval);
            long nextFireTimestamp = start + interval;

            ctx.registerProcessingTimeTimer(nextFireTimestamp);

            fireTimestamp.add(nextFireTimestamp);
            return TriggerResult.CONTINUE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W w, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        //val t = ctx.getCurrentProcessingTime + (10 * 1000 - (ctx.getCurrentProcessingTime % 10 * 1000))
        if (fireTimestamp.get().equals(time)) {
            fireTimestamp.clear();
            fireTimestamp.add(time + interval);
            ctx.registerProcessingTimeTimer(time + interval);
            return TriggerResult.FIRE;
        } else if(w.maxTimestamp() == time) {
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, W w, TriggerContext triggerContext) throws Exception {
        return null;
    }

    @Override
    public void clear(W w, TriggerContext triggerContext) throws Exception {

    }
    private class ReduceMin implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long aLong, Long t1) throws Exception {
            return t1;
        }
    }


}
