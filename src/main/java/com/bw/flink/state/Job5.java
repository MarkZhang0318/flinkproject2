package com.bw.flink.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job5 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Tuple2<Integer, Integer>> ranSource = env.addSource(new RandomNumSource());
        SingleOutputStreamOperator<Tuple2<Integer, String>> aggreSum = ranSource.keyBy(t -> t.f0).flatMap(new AggregatingStateWithCountAvg());
        aggreSum.print();
        env.execute("job5");

    }
}
