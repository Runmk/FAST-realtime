package com.apple.tms.realtime.app.dim;

import com.alibaba.fastjson.JSONObject;
import com.apple.tms.realtime.beans.TmsConfigDimBean;
import com.apple.tms.realtime.common.TmsConfig;
import com.apple.tms.realtime.func.MyBroadcastProcessFunction;
import com.apple.tms.realtime.utils.CreateEnvUtils;
import com.apple.tms.realtime.utils.HbaseUtil;
import com.apple.tms.realtime.utils.KafkaUtils;
import com.google.protobuf.Message;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import javafx.beans.property.SimpleStringProperty;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.eclipse.jetty.util.StringUtil;


public class DimApp {
    public static void main(String[] args) throws Exception {
        //环境准备
        StreamExecutionEnvironment env = CreateEnvUtils.getStreamEnv(args);
        env.setParallelism(4);

        // todo 从kafka 的主题中读取业务数据
        String topic = "tms_ods";
        String groupId = "dim_app_group";
        KafkaSource<String> kafkaSource = KafkaUtils.getKafkaSource(topic, groupId, args);

        SingleOutputStreamOperator<String> kafkaDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source").uid("kafka_source");

        //创建消费者对象


        // todo 对读取的数据进行类型转换并过滤掉不需要传递的json属性
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDs.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String jsonStr) throws Exception {

                JSONObject jsonObject = JSON.parseObject(jsonStr);
                String table = jsonObject.getJSONObject("source").getString("table");
                jsonObject.remove("before");
                jsonObject.remove("source");
                jsonObject.remove("transaction");
                jsonObject.put("table", table);
                return jsonObject;
            }
        });
        jsonObjDS.print(">>>");
        // todo 使用flinkCDC读取配置表数据
        MySqlSource<String> mySqlSource = CreateEnvUtils.getMySqlSource("config_dim", "6000", args);
        SingleOutputStreamOperator<String> mysqlDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource")
                .setParallelism(1)
                .uid("mysql_source");
        mysqlDS.print(">>>>");
        // todo 没有表的情况下创建HBase维度表
        SingleOutputStreamOperator<String> createTableDs = mysqlDS.map(
                new MapFunction<String, String>() {
                    @Override
                    public String map(String jsonStr) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        String op = jsonObject.getString("op");
                        if ("r".equals(op) || "c".equals(op)) {
                            //after 属性的值
                            JSONObject after = jsonObject.getJSONObject("after");
                            String sinkTable = after.getString("sink_table");
                            String sinkFamily = after.getString("sink_family");
                            if (StringUtil.isEmpty(sinkFamily)) {
                                sinkFamily = "info";
                            }
                            System.out.println("在Hbase中创建表：" + sinkTable);
                            HbaseUtil.createTable(TmsConfig.HBASE_NAMESPACE, sinkTable, sinkFamily.split(","));
                        }
                        return jsonStr;
                    }
                }
        );
        // todo 对配置数据进行广播
        MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor
                = new MapStateDescriptor<>("mapStateDescriptor", String.class, TmsConfigDimBean.class);
        BroadcastStream<String> broadcastDS = createTableDs.broadcast(mapStateDescriptor);

        // todo 将主流和广播流进行关联 --connect
        BroadcastConnectedStream<JSONObject, String> connect = jsonObjDS.connect(broadcastDS);

        SingleOutputStreamOperator<JSONObject> dimDS = connect.process(mapStateDescriptor,args);
        // todo 对关联之后的数据进行处理
        jsonObjDS
        // todo 将维度数据保存到Hbase中
        //

        env.execute();
    }
}
