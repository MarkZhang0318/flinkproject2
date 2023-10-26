package com.bw.flink.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
/*
* Session Window
* 需求：当一个单词五秒钟都没有出现的时候将其打出来
* */
public class Job3 {
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
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(1);

        //4.输出结果
        result.print().setParallelism(1);

        //5.启动程序
        env.execute("job1");
    }
}
