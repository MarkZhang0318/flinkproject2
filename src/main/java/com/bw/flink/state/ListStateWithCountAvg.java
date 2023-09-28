package com.bw.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;

public class ListStateWithCountAvg extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Double>> {

    private ListState<Tuple2<Integer, Integer>> countAvg;
    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Tuple2<Integer, Integer>> countAvgDes = new ListStateDescriptor<Tuple2<Integer, Integer>>(
                "count_avg",
                Types.TUPLE(Types.INT, Types.INT)
        );
        countAvg = getRuntimeContext().getListState(countAvgDes);
    }

    @Override
    public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Double>> out) throws Exception {
        Iterable<Tuple2<Integer, Integer>> currentState = countAvg.get();
        if (currentState == null) {
            currentState = Collections.emptyList();
        }
        countAvg.add(value);
        ArrayList<Tuple2<Integer, Integer>> allElements = Lists.newArrayList(countAvg.get());
        if (allElements.size() >= 3) {
            int count = 0;
            int sum = 0;
            for (Tuple2<Integer, Integer> element : allElements) {
                count++;
                sum += element.f1;
            }
            double avg = (double) sum / count;
            out.collect(Tuple2.of(value.f0, avg));

            countAvg.clear();

        }
    }
}
