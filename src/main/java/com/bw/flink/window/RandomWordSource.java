package com.bw.flink.window;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class RandomWordSource implements ParallelSourceFunction<String> {
    private boolean isRunning = true;
    private Random ranNumber;
    private char[] chars = {'a', 'b', 'c', ','};

    private StringBuilder charBuilder;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        System.out.println("run 运行");
        while (isRunning) {
            ranNumber = new Random();
            charBuilder = new StringBuilder();
            /*
            * 首先生成随机数的for循环，用于生成1-10位长度的字符串
            * 再生成随机索引，用string builder取出索引对应的字符
            * 最后返回string builder所保存的字符串
            *
            * */
            for (int i = 0; i < ranNumber.nextInt(10) + 1; i++) {
                int index = ranNumber.nextInt(chars.length);
                charBuilder.append(chars[index]);
            }
            ctx.collect(charBuilder.toString());
            TimeUnit.SECONDS.sleep(1);

        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
