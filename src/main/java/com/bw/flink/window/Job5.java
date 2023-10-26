package com.bw.flink.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/*
* Global Window
*
* */
public class Job5 {
    public static void main(String[] args) throws Exception{
        //1.创建程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //2.添加要处理的数据源
        DataStreamSource<String> source = env.addSource(new RandomWordSource()).setParallelism(1);

        //3.处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(",");
                //Write
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = tuple2SingleOutputStreamOperator
                .keyBy(t -> t.f0)
                /*
                 * Global Window
                 * 需求：当一个单词出现5次后将其打印出来
                 * */
                .window(GlobalWindows.create())
                .trigger(new CountTrigger(2))
                .evictor(new CountEvictor(3))
                .sum(1);

        //4.输出结果
        result.print().setParallelism(1);

        //5.启动程序
        env.execute("job1");
    }

    private static class CountTrigger extends Trigger<Tuple2<String, Integer>, GlobalWindow> {
        long maxCount;

        ReducingStateDescriptor<Long> descriptor = new ReducingStateDescriptor<Long>(
                "count",
                (ReduceFunction<Long>) (value1, value2) -> value1 + value2,
                Long.class
        );

        public CountTrigger(long maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
            ReducingState<Long> reducer = ctx.getPartitionedState(descriptor);
            reducer.add(1L);
            if (reducer.get() == maxCount) {
                reducer.clear();
                //触发计算,并清除窗口内的数据
                //return TriggerResult.FIRE_AND_PURGE;
                //触发计算,但不清除窗口内的数据
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(descriptor).clear();

        }
    }

    private static class CountEvictor implements Evictor<Tuple2<String, Integer>, GlobalWindow> {
        private long maxSize;

        public CountEvictor(long maxSize) {
            this.maxSize = maxSize;
        }

        /**
         *
         * @param elements The elements currently in the pane.
         * @param size The current number of elements in the pane.
         * @param window The {@link Window}
         * @param evictorContext The context for the Evictor
         */

        @Override
        public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
            if (size <= maxSize) {
                return;
            } else {
                //当数据达到了规定大小，需要删除多余的数据了
                Iterator<TimestampedValue<Tuple2<String, Integer>>> iterator = elements.iterator();
                int count = 0;
                while (iterator.hasNext()) {
                    iterator.next();
                    count++;
                    if (count > size - maxSize) {
                        break;
                    } else {
                        //移除
                        iterator.remove();

                    }
                }
            }
        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {

        }
    }

}
