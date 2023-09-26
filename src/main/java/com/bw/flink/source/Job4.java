package com.bw.flink.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source1 = env.addSource(new NotParallelSource());
        DataStreamSource<Long> source2 = env.addSource(new NotParallelSource());

        SingleOutputStreamOperator<String> mapSource = source1.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return value + "";
            }
        });

        DataStream<Long> unionSource = source2.union(source1);
        unionSource.print().setParallelism(1);
        env.execute("Job4");
    }
}
