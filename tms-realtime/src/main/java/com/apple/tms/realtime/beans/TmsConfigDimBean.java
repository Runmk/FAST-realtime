package com.apple.tms.realtime.beans;

import lombok.Data;

@Data
public class TmsConfigDimBean {
    //数据源表名
    String sourceTable;

    //
    String sinkTable;
    //
    String sinkFamily;
    //字段列表
    String sinkColumns;
    //主键
    String sinkPk;
}
