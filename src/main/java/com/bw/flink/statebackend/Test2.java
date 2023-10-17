package com.bw.flink.statebackend;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
* chekcpoint
*
* */
public class Test2 {
    public static void main(String[] args) throws Exception{
        //1.创建程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        env.setStateBackend(new HashMapStateBackend());
        /*
        * 此处创建的时候传入一个url，可以直接将checkpoint存入到HDFS文件系统中
        * */
        checkpointConfig.setCheckpointStorage("hdfs://hadoop5:8020/flink/checkpoint/Test2");

        //设置checkPoint的周期和语义，10秒保存一次，确保所有信息传输到位且只传输一次
        //如果数据量比较大,建议5-10分钟做一次checkpoint
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);


        /*
        * 假设一次checkPoint需要执行15s，但是周期只有10s，此时就会造成checkpoint的积压
        * 利用setMinPauseBetweenCheckpoints方法可以设置checkpoints执行的间隔时间，强制后一个
        * checkpoint等待
        * */
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);

        /*
        *如果checkpoint执行的时间超过了一分钟且还没有执行完，此checkpoint会被放弃
        * setCheckpointTimeout方法设置checkpoint的最大执行时间
        * */
        checkpointConfig.setCheckpointTimeout(60000);

        /*
        * 同一时间只允许一个checkpoint运行
        * */
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        /*
        * 当flink任务取消后，设置是否保存checkpoint里面的数据
        * delete表示不保存，retain表示保存
        * */
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        //2.添加要处理的数据源
        DataStreamSource<String> source = env.socketTextStream("192.168.116.130", 9999);
        //3.处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(",");
                //Write
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(t -> t.f0).sum(1);

        //4.输出结果
        result.print();

        //5.启动程序
        env.execute("job1");
    }

}
