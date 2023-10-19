package com.bw.flink.watermark;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*
* 需求：每隔5秒统计过去10秒内的单词的出现次数
* 5秒表示窗口间隔
* 10秒表示窗口计算长度
* */
public class Job1 {
    public static void main(String[] args) throws Exception {

        //创建程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //创建数据源
        DataStreamSource<String> stringDataStreamSource = env.addSource(new RandomWordSource());
        stringDataStreamSource.print();

        //数据开始处理，统计词频
        stringDataStreamSource.flatMap(new WordCountFlatMap())
                .keyBy(t -> t.f0)
                //.timeWindow(Time.seconds(10), Time.seconds(5))
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(1)
                .print();



        //运行程序
        env.execute("RandomWordCount");
    }

    private static class WordCountFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(",");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));

            }
        }
    }

}

