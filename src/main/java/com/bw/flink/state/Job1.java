package com.bw.flink.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
* 单词计数：每隔三个单词统计一次平均值
* ValueState<T>:这个状态为每一个key保存一个值
*
* */
public class Job1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Tuple2<Integer, Integer>> tuple2DataStreamSource = env.addSource(new RandomNumSource());
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = tuple2DataStreamSource.keyBy(t -> t.f0).flatMap(new ValueStateWithCountAvg());
        stringSingleOutputStreamOperator.print();
        env.execute("job1");
    }
}
