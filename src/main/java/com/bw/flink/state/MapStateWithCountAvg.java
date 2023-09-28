package com.bw.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.UUID;

public class MapStateWithCountAvg extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Double>> {

    private MapState<String, Integer> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Integer> strIntDes = new MapStateDescriptor<String, Integer>(
                "mapState",
                Types.STRING, Types.INT
        );

        mapState = getRuntimeContext().getMapState(strIntDes);
    }

    @Override
    public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Double>> out) throws Exception {
        mapState.put(UUID.randomUUID().toString(), value.f1);

        ArrayList<Integer> elements = Lists.newArrayList(mapState.values());
        if (elements.size() >= 3) {
            int count = 0;
            int sum = 0;
            for (int element : elements) {
                count++;
                sum += element;
            }
            double avg = (double) sum / count;
            out.collect(Tuple2.of(value.f0, avg));

            mapState.clear();
        }
    }
}
