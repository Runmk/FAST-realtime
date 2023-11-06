package com.apple.tms.realtime.func;
import com.apple.tms.realtime.common.TmsConfig;
import com.apple.tms.realtime.utils.HbaseUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.Put;

import java.util.Map;
import java.util.Set;


public class DimSinkFunction implements SinkFunction<JSONObject> {
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        //获取数据目的地表名
        String sinkTable = jsonObject.getString("sink_table");
        jsonObject.remove("sink_table");
        //获取主键自字段
        String sinkPk = jsonObject.getString("sink_pk");
        jsonObject.remove("sink_pk");
        Set<Map.Entry<String, Object>> entrySet = jsonObject.entrySet();
        Put put = new Put(jsonObject.getString(sinkPk).getBytes());
        //遍历
        for (Map.Entry<String, Object> entry : entrySet) {
            if (!sinkPk.equals(entry.getKey())) {
                put.addColumn("info".getBytes(), entry.getKey().getBytes(), entry.getValue().toString().getBytes());
            }
        }
        HbaseUtil.putRow(TmsConfig.HBASE_NAMESPACE,sinkTable,put);
    }
}
