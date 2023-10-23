package com.bw.flink.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
//import org.apache.flink.api.common.eventtime.TimestampAssigner;
//import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Job4 {
    public static void main(String[] args) throws Exception {
        //创建程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //设置处理时间的类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置全局并行度为1
        env.setParallelism(1);

        //数据源
        DataStreamSource<String> source = env.addSource(new TestSource());

        //数据处理
        source.flatMap(new ExtractMapFunction())
                .assignTimestampsAndWatermarks(new EventTimeProcessTmp())
                .keyBy(0)
                .timeWindow(Time.seconds(3))
                .process(new SumProcessTmp())
                .print()
                .setParallelism(1);

        //执行
        env.execute("job6");

    }

    private static class SumProcessTmp extends ProcessWindowFunction<
            Tuple2<String, Long>,
            String,
            Tuple,
            TimeWindow>{
        FastDateFormat fastDateFormat = FastDateFormat.getInstance("HH:mm:ss");
        @Override
        public void process(Tuple tuple, ProcessWindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
            System.out.println("处理时间："+fastDateFormat.format(context.currentProcessingTime()));
            System.out.println("窗口开始时间："+fastDateFormat.format(context.window().getStart()));

            List<String> list = new ArrayList<String>();
            for(Tuple2<String,Long> ele:elements) {
                list.add(ele.toString()+"|"+fastDateFormat.format(ele.f1));
            }
            out.collect(list.toString());
            System.out.println("窗口结束时间："+fastDateFormat.format(context.window().getEnd()));
        }
    }

    private static class EventTimeProcessTmp implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {
        FastDateFormat fastDateFormat = FastDateFormat.getInstance("HH:mm:ss");
        private long currentMaxEventTime = 0L;
        private long maxOutOfOrderness = 10000; //最高延迟时间10m

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            System.out.println(".....执行extractTimestamp......");
            Long currentEventTime = element.f1;
            currentMaxEventTime = Math.max(currentMaxEventTime,currentEventTime);
            System.out.println("event="+element
                    + "|Event Time"+fastDateFormat.format(element.f1)
                    + "|Max Event Time"+fastDateFormat.format(currentMaxEventTime)
                    + "|Current Watermark" + fastDateFormat.format(getCurrentWatermark().getTimestamp())
            );

            return currentEventTime;
        }
    }

    private static FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    private static class ExtractMapFunction implements FlatMapFunction<String, Tuple2<String, Long>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
            String[] fields = value.split(",");

            out.collect(Tuple2.of(fields[0], Long.parseLong(fields[1])));
        }
    }

    private static class TestSource implements SourceFunction<String> {
        FastDateFormat dateFormat1 = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            BufferedReader fileReader;
            String filePath = "datasource.txt";
            fileReader = new BufferedReader(new FileReader(filePath));

            String line;
            while ((line = fileReader.readLine()) != null) {
                ctx.collect(line);
                TimeUnit.SECONDS.sleep(1);

            }

            fileReader.close();

        }

        @Override
        public void cancel() {

        }
    }
}
