package com.apple.tms.realtime.app.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.apple.tms.realtime.utils.CreateEnvUtils;
import com.apple.tms.realtime.utils.KafkaUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.jline.utils.Log;

public class OdsApp {
    public static void main(String[] args) throws Exception {

        // 获取流处理环境
        StreamExecutionEnvironment env = CreateEnvUtils.getStreamEnv(args);
        env.setParallelism(4);

        String dwdOption = "dwd";
        String dwdServerId = "6030";
        String dwdSourceName = "ods_app_dwd_source";
        mysqlToKafka(dwdOption,dwdServerId,dwdSourceName,env,args);


        String realtimeDimOption = "realtime_dim";
        String realtimeDimServerId = "6040";
        String realtimeDimSourceName = "ods_app_realtimeDim_source";
        mysqlToKafka(realtimeDimOption,realtimeDimServerId,realtimeDimSourceName,env,args);
        env.execute();
}

    public static void mysqlToKafka(String option ,String serverId,String sourceName,StreamExecutionEnvironment env,String[] args) {

        MySqlSource<String> mySqlSource = CreateEnvUtils.getMySqlSource(option, serverId, args);
        SingleOutputStreamOperator<String> strDs = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), sourceName)
                .setParallelism(1)
                .uid(option + sourceName);
        //TODO 简单清洗
//        格式不完整的数据。操作类型为删除的数据
        SingleOutputStreamOperator<String> process = strDs.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(jsonStr);
                            if (jsonObject.getJSONObject("after") != null && !"d".equals(jsonObject.getString("op"))) {

                                Long tsMs = jsonObject.getLong("ts_ms");
                                jsonObject.put("ts", tsMs);
                                jsonObject.remove("ts_ms");
                                collector.collect(jsonObject.toJSONString());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            Log.error("从flink 拿到的数据不是一个标准json");
                        }
                    }
                }
        ).setParallelism(1);

        //todo 按照主键进行分组，避免出现乱序 将数据写道kafka主题中
        KeyedStream<String, String> keyedStream = process.keyBy(
                new KeySelector<String, String>() {
                    @Override
                    public String getKey(String jsonStr) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(jsonStr);

                        return jsonObject.getJSONObject("after").getString("id");

                    }
                }
        );

        //kafka
        keyedStream
                .sinkTo(KafkaUtils.getKafkaSink("tms_ods",args) )
                .uid(option + "_ods_app_sink");
    }
    }
