package com.apple.tms.realtime.beans;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author Felix
 * @date 2023/5/31
 * 维度关联需要实现的接口
 */
public interface DimJoinFunction<T> {
    void join(T obj, JSONObject dimInfoJsonObj) ;

    Tuple2<String, String> getCondition(T obj) ;
}
