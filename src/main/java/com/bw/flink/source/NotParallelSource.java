package com.bw.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class NotParallelSource implements SourceFunction<Long> {
    private long number = 10L;
    private boolean isRunning = true;
    /*
    * 产生数据
    * */
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        System.out.println("run 运行");
        while (isRunning) {
            ctx.collect(number);
            number++;
            Thread.sleep(1000);

        }
    }
    /*
    * 程序结束的时候需要什么
    * */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
