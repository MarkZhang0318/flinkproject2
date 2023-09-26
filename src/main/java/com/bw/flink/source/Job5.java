package com.bw.flink.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class Job5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source1 = env.addSource(new NotParallelSource());
        DataStreamSource<Long> source2 = env.addSource(new NotParallelSource());

        SingleOutputStreamOperator<String> mapSource = source2.flatMap(new FlatMapFunction<Long, String>() {
            @Override
            public void flatMap(Long value, Collector<String> out) throws Exception {
                out.collect(value + "");
            }
        });

        ConnectedStreams<Long, String> connectSource = source1.connect(mapSource);
        SingleOutputStreamOperator<Object> result = connectSource.flatMap(new CoFlatMapFunction<Long, String, Object>() {
            @Override
            public void flatMap1(Long value, Collector<Object> out) throws Exception {
                out.collect(value + 1);

            }

            @Override
            public void flatMap2(String value, Collector<Object> out) throws Exception {
                out.collect(value + 1);

            }
        });

        result.print().setParallelism(1);
        env.execute("Job5");
    }
}
