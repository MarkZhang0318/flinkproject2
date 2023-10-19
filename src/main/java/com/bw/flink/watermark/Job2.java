package com.bw.flink.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
* 仔细观察窗口的计算时间
*
* */
public class Job2 {
    private static FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

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
                //.sum(1)
                .process(new SumProcessWindowFunction())
                .print().setParallelism(1);



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

    /*
    *   <IN> The type of the input value.
        <OUT> The type of the output value.
        <KEY> The type of the key.
        <W> The type of {@code Window} that this window function can be applied on.
    * */
    private static class SumProcessWindowFunction extends ProcessWindowFunction<
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            String,
            TimeWindow> {
        @Override
        public void process(String key,
                            ProcessWindowFunction<
                                    Tuple2<String, Integer>,
                                    Tuple2<String, Integer>,
                                    String,
                                    TimeWindow
                                    >.Context context,
                            Iterable<Tuple2<String, Integer>> elements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            //获取系统时间和窗口的过程时间

            System.out.println("当前系统时间：" + dateFormat.format(System.currentTimeMillis()));
            System.out.println("窗口处理时间：" + dateFormat.format(context.currentProcessingTime()));
            System.out.println("窗口开始时间：" + dateFormat.format(context.window().getStart()));
            /*
            * 首先创建计数器，然后遍历传进来的iterator，获取里面的元素
            * 然后利用sum计数器统计里面的值的总数
            * 最后利用collector返回，返回的也应该是一个tuple2
            * 提前用过keyby了，因此所有的key都应该是一个值
            *
            * */
            int sum = 0;
            for (Tuple2<String, Integer> ele : elements) {
                sum += ele.f1;
            }
            out.collect(Tuple2.of(key, sum));
            System.out.println("窗口结束时间：" + dateFormat.format(context.window().getEnd()));
        }
    }

}

