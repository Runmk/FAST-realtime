package com.apple.tms.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.types.Field;

public class KafkaUtils {
        private static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092,";
    public static KafkaSink<String> getKafkaSink(String topic,String transIdPrefix,String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
         topic = parameterTool.get("topic", topic);
        if (topic == null) {
            throw new IllegalArgumentException("主题名不能为null:命令行传参数为空且没有默认值");
        }
        String bootstrapServers = parameterTool.get("bootstrap-servers", KAFKA_SERVER);
        String transactionTimeout = parameterTool.get("transaction-timeout", 15*60*1000+"");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(transIdPrefix)
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,transactionTimeout)
                .build();
        return kafkaSink;
    }

    public static KafkaSink<String> getKafkaSink(String topic, String[] args) {
       return getKafkaSink(topic, topic + " trans", args);
    }
}
