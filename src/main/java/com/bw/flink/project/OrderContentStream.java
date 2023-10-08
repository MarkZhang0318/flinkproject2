package com.bw.flink.project;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class OrderContentStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.获取数据源
        DataStreamSource<String> source1 = env.addSource(new FileSource(Content.filePath1));
        DataStreamSource<String> source2 = env.addSource(new FileSource(Content.filePath2));

        //2.利用包装类中的方法处理数据,并根据id的值进行分组
        KeyedStream<OrderData01, String> orDa01Stream = source1.map(OrderData01::textToOrderData01).keyBy(OrderData01::getOrderId);
        KeyedStream<OrderData02, String> orDa02Stream = source2.map(OrderData02::textToOrderData02).keyBy(OrderData02::getOrderId);

        //3.利用collect方法将俩哥哥数据流连接起来
        orDa01Stream.connect(orDa02Stream).flatMap(new OrderDataStreamFunction()).print();

        //执行
        env.execute("OrderContentStream");

    }

    private static class OrderDataStreamFunction extends RichCoFlatMapFunction<OrderData01, OrderData02,String> {

        private ValueState<OrderData01> orderData01Value;
        private ValueState<OrderData02> orderData02Value;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<OrderData01> orderData01ValDes = new ValueStateDescriptor<>(
                    "OrderData01Value",
                    OrderData01.class
            );

            orderData01Value = getRuntimeContext().getState(orderData01ValDes);

            ValueStateDescriptor<OrderData02> orderData02ValDes = new ValueStateDescriptor<>(
                    "OrderData02Value",
                    OrderData02.class
            );

            orderData02Value = getRuntimeContext().getState(orderData02ValDes);

        }
        /*
         * collect连接的两个流，可以共享一个状态
         * */

        @Override
        public void flatMap1(OrderData01 value, Collector<String> out) throws Exception {
            OrderData02 orderData02 = orderData02Value.value();
            if (orderData02 != null) {
                StringBuilder resultBuilder = new StringBuilder();
                resultBuilder.append("(").append(value.getOrderId()).append(",")
                        .append(value.getProductName()).append(",")
                        .append(value.getPrice()).append(",")
                        .append(orderData02.getCreateTime()).append(",")
                        .append(orderData02.getAddress())
                        .append(")");
                out.collect(resultBuilder.toString());
                orderData02Value.clear();

            } else {
                orderData01Value.clear();
                orderData01Value.update(value);
            }
        }

        @Override
        public void flatMap2(OrderData02 value, Collector<String> out) throws Exception {
            OrderData01 orderData01 = orderData01Value.value();
            if (orderData01 != null) {
                StringBuilder resultBuilder = new StringBuilder();
                resultBuilder.append("(").append(orderData01.getOrderId()).append(",")
                        .append(orderData01.getProductName()).append(",")
                        .append(orderData01.getPrice()).append(",")
                        .append(value.getCreateTime()).append(",")
                        .append(value.getAddress())
                        .append(")");
                out.collect(resultBuilder.toString());
            } else {
                orderData02Value.clear();
                orderData02Value.update(value);

            }
        }


    }

}
