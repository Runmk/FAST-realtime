package com.apple.tms.realtime.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.apple.tms.realtime.beans.TmsConfigDimBean;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.http.util.Args;

import java.sql.*;
import java.util.*;

public class MyBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private  MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor;
    private Map<String, TmsConfigDimBean> configMap = new HashMap<>();
    private String username;
    private String password;

    public MyBroadcastProcessFunction(MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor,String[] args) {
        this.mapStateDescriptor = mapStateDescriptor;
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        this.username = parameterTool.get("mysql-username", "root");
        this.password = parameterTool.get("mysql-password", "123456");

    }


    //将配置表数据进行预加载
    @Override
    public void open(Configuration parameters) throws Exception {
        //注册驱动
        Class.forName("com.mysql.jdbc.Driver");
        // 建立链接
        String url = "jdbc:mysql://hadoop102:3306/tms_config?useSSL=false&useUnicode=true" +
                "&user=" + username + "&password=" + password +
                "&charset=utf8&TimeZone=Asia/shanghai";
        Connection conn = DriverManager.getConnection(url);
        //获取数据库操作对象
        PreparedStatement ps = conn.prepareStatement("select * from tms_config.tms_config_dim");
        // 执行sql语句
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        //处理结果集
        while (rs.next()) {
            JSONObject jsonObject = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                jsonObject.put(columnName, columnValue);
            }
            TmsConfigDimBean tmsConfigDimBean = jsonObject.toJavaObject(TmsConfigDimBean.class);
            configMap.put(tmsConfigDimBean.getSourceTable(), tmsConfigDimBean);
        }
        //释放资源
        rs.close();
        ps.close();
        conn.close();

    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> out) throws Exception {
        //获取操作的业务数据表表名
        String after = jsonObject.getString("table");
        ReadOnlyBroadcastState<String, TmsConfigDimBean> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);


        TmsConfigDimBean tmsConfigDimBean = null;




        if ((tmsConfigDimBean = broadcastState.get(after)) != null || (tmsConfigDimBean = configMap.get(after)) != null) {
            //不为空说明是维度数据
            JSONObject afterJsonObj = jsonObject.getJSONObject("after");
            //过滤不需要传递的维度属性
            String sinkColumns = tmsConfigDimBean.getSinkColumns();
            filterColumn(afterJsonObj, sinkColumns);
            //补充输出的目的地表名
            String sinkTable = tmsConfigDimBean.getSinkTable();
            afterJsonObj.put("sink_table", sinkTable);
            //补充rowKey字段
            String sinkPk = tmsConfigDimBean.getSinkPk();
            afterJsonObj.put("sink_pk", sinkPk);

            //维度数据传递到下游
            out.collect(afterJsonObj);
        }

    }

    private void filterColumn(JSONObject afterJsonObj, String sinkColumns) {
        String[] fieldArr = sinkColumns.split(",");
        List<String> filedList = Arrays.asList(fieldArr);
        Set<Map.Entry<String, Object>> entrySet = afterJsonObj.entrySet();
        entrySet.removeIf(entry -> !filedList.contains(entry.getKey()));
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
            configMap.remove(sourceTable);
        } else {
            TmsConfigDimBean after = jsonObject.getObject("after", TmsConfigDimBean.class);
            //获取维度表名
            String sourceTable = after.getSourceTable();
            broadcastState.put(sourceTable, after);
            configMap.put(sourceTable, after);
        }
    }
}
