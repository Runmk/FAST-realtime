package com.apple.tms.realtime.app.dim;
import com.apple.tms.realtime.utils.CreateEnvUtils
import com.apple.tms.realtime.utils.KafkaUtils;
import com.google.protobuf.Message;
import javafx.beans.property.SimpleStringProperty;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import javax.script.SimpleScriptContext;
import java.io.IOException;

public class DimApp {
    public static void main(String[] args) throws Exception {
    //环境准备
        StreamExecutionEnvironment env = CreateEnvUtils.getStreamEnv(args);
        env.setParallelism(4);

        // todo 从kafka 的主题中读取业务数据
        String topic = "tms_ods";
        String groupId = "dim_app_group";
        KafkaSource<String> kafkaSource = KafkaUtils.getKafkaSource(topic, groupId, args);

        SingleOutputStreamOperator<String> kafkaDs = env.
                fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .uid("kafka_source");

        //创建消费者对象


        // todo 对读取的数据进行类型转换并过滤掉不需要传递的json属性
    // todo 使用flinkCDC读取配置表数据
    // todo 没有表的情况下创建HBase维度表
    // todo 对配置数据进行广播
    // todo 将主流和广播流进行关联 --connect
    // todo 对关联之后的数据进行处理
    // todo 将维度数据保存到Hbase中
    //

        env.execute();
    }
}
