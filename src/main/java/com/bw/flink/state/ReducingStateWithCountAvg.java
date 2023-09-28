package com.bw.flink.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class ReducingStateWithCountAvg extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Double>> {
    private ReducingState<Integer> reducingState;
    private ValueState<Integer> countState;
    @Override
    public void open(Configuration parameters) throws Exception {
        ReducingStateDescriptor<Integer> redStaDes = new ReducingStateDescriptor<Integer>(
                "reducingState",
                new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 + value2;
                    }
                },
                Integer.class
        );
        reducingState = getRuntimeContext().getReducingState(redStaDes);

        ValueStateDescriptor<Integer> countStateDes = new ValueStateDescriptor<>(
                "countState",
                Integer.class
        );
        countState = getRuntimeContext().getState(countStateDes);
    }

    @Override
    public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Double>> out) throws Exception {
        Integer count = countState.value();
        count = (count == null) ? 1 : count + 1;
        countState.update(count);
        reducingState.add(value.f1);
        if (count >= 3) {
            double avg = (double)reducingState.get() / count;
            out.collect(Tuple2.of(value.f0, avg));
            countState.clear();
            reducingState.clear();

        }
    }
}
