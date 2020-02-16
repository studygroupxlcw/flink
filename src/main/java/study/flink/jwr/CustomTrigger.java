package jwr;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

public  class CustomTrigger<W extends Window> extends Trigger<Object, W> {
    // 触发的条数
    private final long size;
    // 触发的时长
    private final long interval;
    private static final long serialVersionUID = 1L;
    // 条数计数器
    private final ReducingStateDescriptor<Long> countStateDesc =
            new ReducingStateDescriptor<>("count", new ReduceSum(), LongSerializer.INSTANCE);
    // 时间计数器，保存下一次触发的时间
    private final ReducingStateDescriptor<Long> timeStateDesc =
            new ReducingStateDescriptor<>("fire-interval", new ReduceMin(), LongSerializer.INSTANCE);

    public CustomTrigger(long size, long interval) {
        this.size = size;
        this.interval = interval;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        // 注册窗口结束的触发器, 不需要会自动触发
//        ctx.registerProcessingTimeTimer(window.maxTimestamp());
        // count
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
        //interval
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(timeStateDesc);
        // 每条数据 counter + 1
        count.add(1L);
        if (count.get() >= size) {
            // 满足条数的触发条件，先清 0 条数计数器
            count.clear();
            // 满足条数时也需要清除时间的触发器，如果不是创建结束的触发器
            if (fireTimestamp.get() != window.maxTimestamp()) {
//                logger.info("delete trigger : {}, {}", sdf.format(fireTimestamp.get()), fireTimestamp.get());
                ctx.deleteProcessingTimeTimer(fireTimestamp.get());
            }
            fireTimestamp.clear();
            // fire 触发计算
            return TriggerResult.FIRE;
        }

        // 触发之后，下一条数据进来才设置时间计数器注册下一次触发的时间
        timestamp = ctx.getCurrentProcessingTime();
        if (fireTimestamp.get() == null) {
//            long start = timestamp - (timestamp % interval);
            long nextFireTimestamp = timestamp + interval;
//            logger.info("register trigger : {}, {}", sdf.format(nextFireTimestamp), nextFireTimestamp);
            ctx.registerProcessingTimeTimer(nextFireTimestamp);
            fireTimestamp.add(nextFireTimestamp);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {

        // count
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
        //interval
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(timeStateDesc);

        // time trigger and window end
        if (time == window.maxTimestamp()) {
            // 窗口结束，清0条数和时间的计数器
            count.clear();
            ctx.deleteProcessingTimeTimer(fireTimestamp.get());
            fireTimestamp.clear();
            return TriggerResult.FIRE_AND_PURGE;
        } else if (fireTimestamp.get() != null && fireTimestamp.get().equals(time)) {
            // 时间计数器触发，清0条数和时间计数器
            count.clear();
            fireTimestamp.clear();
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


    private class ReduceSum implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long aLong, Long t1) throws Exception {
            return aLong+t1;
        }
    }

    private class ReduceMin implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long aLong, Long t1) throws Exception {
            return t1;
        }
    }
}
