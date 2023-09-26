package com.bw.flink.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class ParallelSource implements ParallelSourceFunction<Long> {
    private Long number = 10L;
    private boolean isRunning = true;
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        System.out.println("run 运行");
        while (isRunning) {
            ctx.collect(number);
            number++;
            Thread.sleep(1000);

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
