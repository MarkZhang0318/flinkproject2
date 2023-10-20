package com.bw.flink.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

/*
* 仔细观察窗口的计算时间
*
* */
public class Job3 {
    private static FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    public static void main(String[] args) throws Exception {

        //创建程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //设置事件类型为EventTime，默认是ProcessingTime
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //创建数据源
        DataStreamSource<String> stringDataStreamSource = env.addSource(new TestSource());
        //stringDataStreamSource.print();
        //创建WaterMarkStrategy
        WatermarkStrategy<Tuple2<String, Long>> tuple2WatermarkStrategy = WatermarkStrategy
                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                //.noWatermarks()
                .withTimestampAssigner((event, timestamp) -> event.f1);


        //数据开始处理，统计词频
        stringDataStreamSource
                .flatMap(new extractMapFunction())
                .assignTimestampsAndWatermarks(tuple2WatermarkStrategy)
                .keyBy(t -> t.f0)
                //.timeWindow(Time.seconds(10), Time.seconds(5))
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                //.sum(1)
                .process(new SumProcessWindowFunction())
                .print()
        .setParallelism(1)
        ;



        //运行程序
        env.execute("WindowWordCount");
    }

    private static class TestSource implements SourceFunction<String> {
        FastDateFormat dateFormat1 = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            /*
            * 控制程序开始时间，将秒控制在10的倍数秒开始
            * */
            LocalTime now;
            int seconds;
            while (true) {
                now = LocalTime.now();
                seconds = now.getSecond();
                if (seconds % 10 == 0) {
                    break;
                }
                TimeUnit.SECONDS.sleep(1);
            }
            System.out.println("开始发送事件的时间：" + dateFormat.format(System.currentTimeMillis()));
            /*
            * 模拟以下使用场景：
            * 原计划3秒的时候发送两条消息，6秒的时候发送1条消息
            * 由于网络波动，3秒发送的消息直到9秒才收到
            * */
            TimeUnit.SECONDS.sleep(3);
            String event = "flink," + System.currentTimeMillis();
            ctx.collect(event);

            TimeUnit.SECONDS.sleep(3);
            ctx.collect("flink," + System.currentTimeMillis());

            TimeUnit.SECONDS.sleep(3);
            ctx.collect(event);

            TimeUnit.SECONDS.sleep(10);

        }

        @Override
        public void cancel() {

        }
    }

    private static class EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            /*
            * 此处给时间窗口添加5秒的延迟，以保证能接收到
            * 因为网络波动而延迟到达的事件
            * */
            return new Watermark(System.currentTimeMillis() - 5000);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
            /*
            * 此处预计会接收到传入的字符串所形成的tuple，其中第二个字段就是
            * 事件产生时的时间戳
            * */
            return element.f1;
        }

    }

    private static class extractMapFunction implements FlatMapFunction<String, Tuple2<String, Long>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
            String[] words = value.split(",");
            out.collect(Tuple2.of(words[0], Long.parseLong(words[1])));

        }
    }

    /*
    *   <IN> The type of the input value.
        <OUT> The type of the output value.
        <KEY> The type of the key.
        <W> The type of {@code Window} that this window function can be applied on.
    * */
    private static class SumProcessWindowFunction extends ProcessWindowFunction<
            Tuple2<String, Long>,
            Tuple2<String, Integer>,
            String,
            TimeWindow> {
        @Override
        public void process(String key,
                            ProcessWindowFunction<
                                    Tuple2<String, Long>,
                                    Tuple2<String, Integer>,
                                    String,
                                    TimeWindow
                                    >.Context context,
                            Iterable<Tuple2<String, Long>> elements,
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
            for (Tuple2<String, Long> ele : elements) {
                sum++;
            }
            out.collect(Tuple2.of(key, sum));
            System.out.println("窗口结束时间：" + dateFormat.format(context.window().getEnd()));
            System.out.println("===================================");
        }
    }

}

