package com.bw.flink.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job1 {
    public static void main(String[] args) throws Exception{
        //创建程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //创建程序源
        DataStreamSource source = env.addSource(new NotParallelSource());

        //数据处理
        SingleOutputStreamOperator filterResult = source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        });

        //数据输出
        filterResult.print().setParallelism(1);
        env.execute("job1");

    }
}
