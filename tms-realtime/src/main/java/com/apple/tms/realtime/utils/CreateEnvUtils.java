package com.apple.tms.realtime.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.jline.utils.Log;

import java.util.HashMap;

public class CreateEnvUtils {
    //流处理环境
    public static StreamExecutionEnvironment getStreamEnv(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        //set checkpoint args
        env.enableCheckpointing(6000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(12000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1),Time.seconds(3)));
        //设置状态后端 ???
        env.getStateBackend();
        env.getCheckpointConfig().setCheckpointStorage("hdfs:/");
        //设置操作hfds的用户
        //获取命令行参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hdfsUserName = parameterTool.get("HADOOP_UESR_NAME", "atguigu");

        System.setProperty("HADOOP_UESR_NAME",hdfsUserName );
        return env;
    }
    //获取mysqlSource

    public static MySqlSource<String>  getMySqlSource(String option , String serverId, String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostName = parameterTool.get("HOSTNAME", "hadoop102");
        String password = parameterTool.get("PASSWORD", "123456");
        String userName = parameterTool.get("USERNAME", "hadoop102");
        int mysqlPort = Integer.parseInt(parameterTool.get("MYSQL_PORT", "3306"));

        serverId = parameterTool.get("serverId", serverId);
        option = parameterTool.get("START_UP_OPTIONS", option);

        //创建配置信息map集合，将Decimal 数据类型的解析格式配置成k-v 至于其中
        HashMap config = new HashMap<>();
        config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        JsonDebeziumDeserializationSchema jsonDebeziumDeserializationSchema = new JsonDebeziumDeserializationSchema(false, config);


        MySqlSourceBuilder<String> builder = MySqlSource.<String>builder()
                .hostname(hostName)
                .port(mysqlPort)
                .username(userName)
                .password(password)
                .deserializer(jsonDebeziumDeserializationSchema);

        switch (option) {
            case "dwd":
                String[] dwdTables = new String[]{"tms.order_info",
                        "tims.order_cargo",
                        "tms.transport_task",
                        "tms.order_org_bound"
                };
                builder.databaseList("tms")
                        .tableList(dwdTables)
                        .startupOptions(StartupOptions.latest())
                        .serverId(serverId)
                        .build();
            case "realtime_dim":
                String[] dimTables = new String[]{"tms.user_info",
                        "tms.user_address",
                        "tms.base_camplex",
                        "tms.base_dic",
                        "tms.base_region_info",
                        "tms.base_organ",
                        "tms.express_courier",
                        "tms.express_courier_complex",
                        "tms.employee_info",
                        "tms.line_base_shift",
                        "tms.line_base_info",
                        "tms.truck_driver",
                        "tms.truck_info",
                        "tms.truck_model",
                        "tms.truck_team",
                };
                builder.databaseList("tms")
                        .tableList(dimTables)
                        .startupOptions(StartupOptions.initial())
                        .serverId(serverId)
                        .build();
        }


        Log.error("不支持的操作类型");
        return null;
    }
}
