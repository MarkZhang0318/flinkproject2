package com.bw.flink.state;

import com.bw.flink.source.ParallelSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class RandomNumSource implements ParallelSourceFunction<Tuple2<Integer, Integer>> {

    private Random ran = new Random();
    private boolean isRunning = true;


    @Override
    public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(new Tuple2<>(ran.nextInt(10), ran.nextInt(500)));

            TimeUnit.SECONDS.sleep(1);

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
