package com.bw.flink.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class AggregatingStateWithCountAvg extends RichFlatMapFunction<Tuple2<Integer,Integer>, Tuple2<Integer,String>> {
    private AggregatingState<Integer, String> aggregatingState;
    private ValueState<Integer> countState;
    @Override
    public void open(Configuration parameters) throws Exception {
        AggregatingStateDescriptor<Integer, String, String> aggregatingStateDescriptor = new AggregatingStateDescriptor<>(
                "aggregatingState",
                new AggregateFunction<Integer, String, String>() {
                    @Override
                    public String createAccumulator() {
                        return "";
                    }

                    @Override
                    public String add(Integer value, String accumulator) {
                        if (accumulator.length() == 0) {
                            return accumulator + value;
                        } else {
                            return accumulator + " - " + value;
                        }
                    }

                    @Override
                    public String getResult(String accumulator) {
                        return accumulator;
                    }

                    @Override
                    public String merge(String a, String b) {
                        return a + " - " + b;
                    }
                },
                String.class
        );
        aggregatingState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);

        ValueStateDescriptor<Integer> countStateDes = new ValueStateDescriptor<>(
                "countState",
                Integer.class
        );
        countState = getRuntimeContext().getState(countStateDes);
    }

    @Override
    public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, String>> out) throws Exception {
        aggregatingState.add(value.f1);
        Integer count = countState.value();
        count = (count == null) ? 1 : count + 1;
        countState.update(count);

        if (count >= 3) {
            out.collect(Tuple2.of(value.f0, aggregatingState.get()));

            aggregatingState.clear();
            countState.clear();

        }
    }
}
