package com.bw.flink.project;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.protocol.types.Field;
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderData02 {
    private String orderId;
    private String createTime;
    private String address;

    public static OrderData02 textToOrderData02(String line) {
        OrderData02 orderData02 = null;
        String[] fields = line.split(",");
        orderData02 = new OrderData02(fields[0], fields[1], fields[2]);
        return orderData02;
    }

}
