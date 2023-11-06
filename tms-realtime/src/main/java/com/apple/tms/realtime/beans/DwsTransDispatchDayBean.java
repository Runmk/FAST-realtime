package com.apple.tms.realtime.beans;

import lombok.Builder;
import lombok.Data;

/**
 * @author Felix
 * @date 2023/6/2
 * 物流域发单统计实体类
 */
@Data
@Builder
public class DwsTransDispatchDayBean {
    // 统计日期
    String curDate;

    // 发单数
    Long dispatchOrderCountBase;

    // 时间戳
    Long ts;
}
