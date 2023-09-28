package com.bw.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class ValueStateWithCountAvg extends RichFlatMapFunction<Tuple2<Integer, Integer>,String> {
    private ValueState<Integer> countState;
    private ValueState<Integer> valueAccumulate;
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化计数器
        ValueStateDescriptor<Integer> countStateDes = new ValueStateDescriptor<>(
                "countState",
                Integer.class
        );
        countState = getRuntimeContext().getState(countStateDes);

        //初始化累加器
        ValueStateDescriptor<Integer> valueAccumulateDes = new ValueStateDescriptor<>(
                "valueAccumulate",
                Integer.class
        );
        valueAccumulate = getRuntimeContext().getState(valueAccumulateDes);
    }

    @Override
    public void flatMap(Tuple2<Integer, Integer> value, Collector<String> out) throws Exception {
        //获取计数器和累加器当前的值
        Integer count = countState.value();
        Integer accumulator = valueAccumulate.value();

        //更新计数值和累加值
        count = (count == null) ? 1 : count + 1;
        accumulator = (accumulator == null) ? value.f1 : accumulator + value.f1;

        //将计数和累加结果更新到state
        countState.update(count);
        valueAccumulate.update(accumulator);

        //如果key值出现超过3次,执行计算并输出结果
        if (count >= 3) {
            double average = (double) accumulator / count;
            out.collect("key:" + value.f0 + ", average valye:" + average);

            countState.clear();
            valueAccumulate.clear();
        }
    }
}
