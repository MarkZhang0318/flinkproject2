package com.bw.flink.project;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.protocol.types.Field;

@Data
@AllArgsConstructor
@NoArgsConstructor

public class OrderData01 {
    private String orderId;
    private String productName;
    private double price;

    public static OrderData01 textToOrderData01(String line) {
        OrderData01 orderData01 = null;
        String[] fields = line.split(",");
        orderData01 = new OrderData01(fields[0], fields[1], Double.parseDouble(fields[2]));
        return orderData01;
    }
}
