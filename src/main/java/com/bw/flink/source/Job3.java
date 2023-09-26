package com.bw.flink.source;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Job3 {
    private static final String[] TYPE = {"火龙果", "苹果", "梨", "葡萄", "香蕉", "橘子"};

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> tuple2DataStreamSource = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            private boolean isRunning = true;
            private Random random = new Random();

            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                while (isRunning) {
                    ctx.collect(new Tuple2<String, Integer>(
                            TYPE[random.nextInt(TYPE.length)],
                            1
                    ));
                    TimeUnit.SECONDS.sleep(2);//相当于Thread.sleep,不过此处直接转换为秒

                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceResult = tuple2DataStreamSource.keyBy(t -> t.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1);
            }

        });

        reduceResult.print().setParallelism(1);
        env.execute("job3");
    }
}
