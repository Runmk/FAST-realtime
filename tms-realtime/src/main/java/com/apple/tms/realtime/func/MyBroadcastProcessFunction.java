package com.apple.tms.realtime.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.apple.tms.realtime.beans.TmsConfigDimBean;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class MyBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private  MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor;

    public MyBroadcastProcessFunction(MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

    }

    @Override
    public void processBroadcastElement(String jsonStr, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
//        System.out.println(jsonStr);
        BroadcastState<String, TmsConfigDimBean> broadcastState = context.getBroadcastState(mapStateDescriptor);
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        //获取广播状态

        String op = jsonObject.getString("OP");
        if ("d".equals(op)) {
            String sourceTable = jsonObject.getJSONObject("before").getString("source_table");
            broadcastState.remove(sourceTable);
        } else {
            TmsConfigDimBean after = jsonObject.getObject("after", TmsConfigDimBean.class);
            //获取维度表名
            String sourceTable = after.getSourceTable();
            broadcastState.put(sourceTable, after);
        }
    }
}
