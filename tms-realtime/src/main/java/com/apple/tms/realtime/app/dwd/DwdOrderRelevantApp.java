package com.apple.tms.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.apple.tms.realtime.beans.*;
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
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdOrderRelevantApp {
    public static void main(String[] args) {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = CreateEnvUtils.getStreamEnv(args);
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO 2.从kafka的tms_ods主题中读取
        String topic = "tms_ods";
        String groupId = "dwd_order_relevant_group";

        KafkaSource<String> kafkaSource = KafkaUtils.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> kafkaStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        SingleOutputStreamOperator<String> filterDS = kafkaStrDS.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String JsonStr) throws Exception {
                        val jsonObj = JSON.parseObject(JsonStr);
                        String tableName = jsonObj.getJSONObject("source").getString("table");
                        return "order_info".equals(tableName) || "order_cargo".equals(tableName);
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> jsonObjDS = filterDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String jsonStr) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        String tableName = jsonObject.getJSONObject("source").getString("table");
                        jsonObject.put("table", tableName);
                        jsonObject.remove("source");
                        jsonObject.remove("transaction");
                        return jsonObject;
                    }
                }
        );


        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(
                new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObj) throws Exception {
                String table = jsonObj.getString("table");
                if ("order_info".equals(table)) {
                    return jsonObj.getJSONObject("after").getString("id");
                }
                return jsonObj.getJSONObject("after").getString("order_id");
            }
        });
        //TODO 6.定义侧输出流标签 下单放到主流，支付成功、取消运单、揽收(接单)、发单 转运完成、派送成功、签收放到侧输出流

        // 支付成功明细流标签
        OutputTag<String> paySucTag = new OutputTag<String>("dwd_trade_pay_suc_detail") {
        };
        // 取消运单明细流标签
        OutputTag<String> cancelDetailTag = new OutputTag<String>("dwd_trade_cancel_detail") {
        };
        // 揽收明细流标签
        OutputTag<String> receiveDetailTag = new OutputTag<String>("dwd_trans_receive_detail") {
        };
        // 发单明细流标签
        OutputTag<String> dispatchDetailTag = new OutputTag<String>("dwd_trans_dispatch_detail") {
        };
        // 转运完成明细流标签
        OutputTag<String> boundFinishDetailTag = new OutputTag<String>("dwd_trans_bound_finish_detail") {
        };
        // 派送成功明细流标签
        OutputTag<String> deliverSucDetailTag = new OutputTag<String>("dwd_trans_deliver_detail") {
        };
        // 签收明细流标签
        OutputTag<String> signDetailTag = new OutputTag<String>("dwd_trans_sign_detail") {
        };

        SingleOutputStreamOperator<String> orderDetailDS =  keyedDS.process(new KeyedProcessFunction<String, JSONObject, String>() {
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
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, String>.Context context, Collector<String> out) throws Exception {
                String table = jsonObject.getString("table");
                String op = jsonObject.getString("op");
                JSONObject data = jsonObject.getJSONObject("after");
                //判断是订单还是明细
                if ("order_info".equals(table)) {

                    //处理的是订单数据
                    DwdOrderInfoOriginBean infoOriginBean = data.toJavaObject(DwdOrderInfoOriginBean.class);

                    // 脱敏
                    String senderName = infoOriginBean.getSenderName();
                    String receiverName = infoOriginBean.getReceiverName();

                    senderName = senderName.charAt(0) + senderName.substring(1).replaceAll(".", "\\*");
                    receiverName = receiverName.charAt(0) + receiverName.substring(1).replaceAll(".", "\\*");

                    infoOriginBean.setSenderName(senderName);
                    infoOriginBean.setReceiverName(receiverName);

                    DwdOrderDetailOriginBean detailOriginBean = detailBeanState.value();
                    if ("c".equals(op)) {
                        //下单
                        if (detailOriginBean == null) {
                            //订单数据笔订单明细数据先到
                            infoBeanState.update(infoOriginBean);
                        }else {
                            DwdTradeOrderDetailBean dwdTradeOrderDetailBean = new DwdTradeOrderDetailBean();
                            dwdTradeOrderDetailBean.mergeBean(detailOriginBean,infoOriginBean);
                            out.collect(JSON.toJSONString(dwdTradeOrderDetailBean));
                        }
                    }  else if ("u".equals(op)) {
                        //其他操作
                        JSONObject before = jsonObject.getJSONObject("before");
                        //修改之前的值
                        String oldStatus = before.getString("status");
                        //修改后的状态值
                        String status = infoOriginBean.getStatus();
                        if (!oldStatus.equals(status)) {
                            //修改的是status
                            String changeLog = oldStatus + " -> " + status;
                            switch (changeLog) {
                                case "60010 -> 60020":
                                    // 处理支付成功数据
                                    DwdTradePaySucDetailBean dwdTradePaySucDetailBean = new DwdTradePaySucDetailBean();
                                    dwdTradePaySucDetailBean.mergeBean(detailOriginBean, infoOriginBean);
                                    context.output(paySucTag, JSON.toJSONString(dwdTradePaySucDetailBean));
                                    break;
                                case "60020 -> 60030":
                                    // 处理揽收明细数据
                                    DwdTransReceiveDetailBean dwdTransReceiveDetailBean = new DwdTransReceiveDetailBean();
                                    dwdTransReceiveDetailBean.mergeBean(detailOriginBean, infoOriginBean);
                                    context.output(receiveDetailTag, JSON.toJSONString(dwdTransReceiveDetailBean));
                                    break;
                                case "60040 -> 60050":
                                    // 处理发单明细数据
                                    DwdTransDispatchDetailBean dispatchDetailBean = new DwdTransDispatchDetailBean();
                                    dispatchDetailBean.mergeBean(detailOriginBean, infoOriginBean);
                                    context.output(dispatchDetailTag, JSON.toJSONString(dispatchDetailBean));
                                    break;
                                case "60050 -> 60060":
                                    // 处理转运完成明细数据
                                    DwdTransBoundFinishDetailBean boundFinishDetailBean = new DwdTransBoundFinishDetailBean();
                                    boundFinishDetailBean.mergeBean(detailOriginBean, infoOriginBean);
                                    context.output(boundFinishDetailTag, JSON.toJSONString(boundFinishDetailBean));
                                    break;
                                case "60060 -> 60070":
                                    // 处理派送成功数据
                                    DwdTransDeliverSucDetailBean dwdTransDeliverSucDetailBean = new DwdTransDeliverSucDetailBean();
                                    dwdTransDeliverSucDetailBean.mergeBean(detailOriginBean, infoOriginBean);
                                    context.output(deliverSucDetailTag, JSON.toJSONString(dwdTransDeliverSucDetailBean));
                                    break;
                                case "60070 -> 60080":
                                    // 处理签收明细数据
                                    DwdTransSignDetailBean dwdTransSignDetailBean = new DwdTransSignDetailBean();
                                    dwdTransSignDetailBean.mergeBean(detailOriginBean, infoOriginBean);
                                    context.output(signDetailTag, JSON.toJSONString(dwdTransSignDetailBean));
                                    // 签收后订单数据不会再发生变化，状态可以清除
                                    detailBeanState.clear();
                                    break;
                                default:
                                    if (status.equals("60999")) {
                                        DwdTradeCancelDetailBean dwdTradeCancelDetailBean = new DwdTradeCancelDetailBean();
                                        dwdTradeCancelDetailBean.mergeBean(detailOriginBean, infoOriginBean);
                                        context.output(cancelDetailTag, JSON.toJSONString(dwdTradeCancelDetailBean));
                                        // 取消后订单数据不会再发生变化，状态可以清除
                                        detailBeanState.clear();
                                    }
                                    break;

                            }
                        }
                    }
                } else {
                    DwdOrderDetailOriginBean dwdOrderDetailOriginBean = data.toJavaObject(DwdOrderDetailOriginBean.class);
                    if ("c".equals(op)) {
                        //将明细数据放到状态中
                        detailBeanState.update(dwdOrderDetailOriginBean);
                        //获取状态中存放的订单数据
                        DwdOderInfoOriginBean infoOriginBean = infoBeanState.value();
                        if (infoOriginBean != null) {
                            DwdTradeOrderDetailBean dwdTradeOrderDetailBean = new DwdTradeOrderDetailBean();
                            dwdTradeOrderDetailBean.mergeBean(detailBeanState,infoOriginBean);
                            //将下单业务过程数据放到主流中
                            out.collect(JSON.toJSONString(dwdTradeOrderDetailBean));
                        }
                    }


                }

                context.output();
            }
        })


    }
}
