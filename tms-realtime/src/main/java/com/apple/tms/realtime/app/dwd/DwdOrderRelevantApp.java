package com.apple.tms.realtime.app.dwd;

import apple.laf.JRSUIState;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.apple.tms.realtime.beans.DwdOderInfoOriginBean;
import com.apple.tms.realtime.beans.DwdOrderDetailOriginBean;
import com.apple.tms.realtime.utils.CreateEnvUtils;
import com.apple.tms.realtime.utils.KafkaUtils;
import lombok.val;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.scheduler.metrics.StateTimeMetric;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdOrderRelevantApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = CreateEnvUtils.getStreamEnv(args);
        env.setParallelism(4);
        //从kafka在tms_ods 中那数据
        String topic = "tms_ods";
        String groupId = "dwd_order_relevant_group";

        KafkaSource<String> kafkaSource = KafkaUtils.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        SingleOutputStreamOperator<String> filterDS = kafkaStrDS.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String JsonStr) throws Exception {
                        val jsonObject = JSON.parseObject(JsonStr);
                        String tableName = jsonObject.getJSONObject("source").getString("table");
                        return "order_info".equals(tableName) || "order_cargo".equals(tableName);
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> jsonObjDS = filterDS.map(

                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String JsonObj) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(JsonObj);
                        String tableName = jsonObject.getJSONObject("source").getString("table");
                        jsonObject.put("table", tableName);
                        jsonObject.remove("source");
                        jsonObject.remove("transaction");
                        return jsonObject;
                    }
                }
        );


        KeyedStream<JSONObject, String> keyed = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObj) throws Exception {
                String table = jsonObj.getString("table");
                if ("order_info".equals(table)) {
                    return jsonObj.getJSONObject("after").getString("id");
                }
                return jsonObj.getJSONObject("after").getString("order_id");
            }
        });
        //测输出流标签
        OutputTag<String> paySucTag = new OutputTag<String>("dwd_trade_pay_suc_detail"){};
        OutputTag<String> cancelDetailTag = new OutputTag<String>("dwd_trade_cancel_detail"){};
        OutputTag<String> receiveDetailTag = new OutputTag<String>("dwd_trans_receive_detail"){};
        OutputTag<String> dispatchDetailTag = new OutputTag<String>("dwd_trans_dispatch_detail"){};
        OutputTag<String> boundFinishDetailTag = new OutputTag<String>("dwd_trans_bound_finish_detail"){};
        OutputTag<String> deliverSucDetailTag = new OutputTag<String>("dwd_trans_deliver_detail"){};
        OutputTag<String> signDetailTag = new OutputTag<String>("dwd_trans_sign_detail"){};
        //分流

        keyed.process(new KeyedProcessFunction<String, JSONObject, String>() {
            private ValueState<DwdOderInfoOriginBean> infoBeanState;
            private ValueState<DwdOrderDetailOriginBean> detailBeanState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<DwdOderInfoOriginBean> infoBeanValueStateDescriptor
                        = new ValueStateDescriptor<DwdOderInfoOriginBean>("infoBeanState", DwdOderInfoOriginBean.class);
                infoBeanValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());

                infoBeanState = getRuntimeContext().getState(infoBeanValueStateDescriptor);

                ValueStateDescriptor<DwdOrderDetailOriginBean> detailBeanStateDescriptor
                        = new ValueStateDescriptor<DwdOrderDetailOriginBean>("detailBeanState",DwdOrderDetailOriginBean.class);

                detailBeanState = getRuntimeContext().getState(detailBeanStateDescriptor);

            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, String>.Context context, Collector<String> collector) throws Exception {


                context.output();
            }
        })


    }
}
