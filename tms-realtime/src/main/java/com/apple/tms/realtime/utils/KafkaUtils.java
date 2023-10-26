package com.apple.tms.realtime.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.types.Field;

import java.io.IOException;

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

    // 获取kafkasource
    public static KafkaSource<String > getKafkaSource (String topic ,String groupId,String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
         topic = parameterTool.get("topic", topic);
        if (topic == null) {
            throw new IllegalArgumentException("主题不能为空");
        }
        String bootstrapServer = parameterTool.get("bootstrap-server", KAFKA_SERVER);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()

                .setBootstrapServers(bootstrapServer)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                // 注意：使用simpleStringSchema 进行反序列化。如果读到的消息为空，处理不了，需要自定义反序列化类
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message);

                        }
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        return kafkaSource;
    }
}
