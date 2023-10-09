package com.bw.flink.project.kafkaConnector;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

@AllArgsConstructor
@Data
public class MyKafkaSource {
    private String brokers;
    private String groupId;
    private String topic;

    public FlinkKafkaConsumer<String> getKafkaSource() {
        FlinkKafkaConsumer<String> myConsumer = null;
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("group.id", groupId);
        myConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
        myConsumer.setStartFromEarliest();
        return myConsumer;
    }

}
