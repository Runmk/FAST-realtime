package com.apple.tms.realtime.app.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.apple.tms.realtime.utils.CreateEnvUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.jline.utils.Log;

public class OdsApp {
    public static void main(String[] args) {

        // 获取流处理环境
        StreamExecutionEnvironment env = CreateEnvUtils.getStreamEnv(args);
        env.setParallelism(4);

        String dwdOption = "dwd";
        String dwdServerId = "6030";
        String sourceName = "ods_app_dwd_source";
        MySqlSource<String> dwdmySqlSource = CreateEnvUtils.getMySqlSource(dwdOption, dwdServerId, args);
        SingleOutputStreamOperator<String> dwdStrDs = env
                .fromSource(dwdmySqlSource, WatermarkStrategy.noWatermarks(), sourceName)
                .setParallelism(1)
                .uid(dwdOption + sourceName);
        //TODO 简单清洗
//        格式不完整的数据。操作类型为删除的数据
        SingleOutputStreamOperator<String> process = dwdStrDs.process(
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
        keyedStream.sinkTo()


    }
}
