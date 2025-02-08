package com.example.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum DataProcessTypeEnum {
    STREAM_PROCESS(0,"流处理", "STREAM", "实时数据流"),
    BATCH_PROCESS(1,"批处理", "BATCH", "离线数据流");

    private final Integer type;
    private final String  desc;
    private final String enFlag;
    private final String dstreamDesc;

    public static DataProcessTypeEnum getProcessType(Integer code) {
        for (DataProcessTypeEnum item : DataProcessTypeEnum.values()) {
            if (item.getType().equals(code)) {
                return item;
            }
        }
        return null;
    }



    public static String getProcessTypeDesc(Integer code) {
        for (DataProcessTypeEnum item : DataProcessTypeEnum.values()) {
            if (item.getType().equals(code)) {
                return item.getDesc();
            }
        }
        return null;
    }

    public static String getDstreamDescByCode(Integer code) {
        if (DataProcessTypeEnum.STREAM_PROCESS.getType().equals(code)) {
            return DataProcessTypeEnum.STREAM_PROCESS.dstreamDesc;
        } else {
            return DataProcessTypeEnum.BATCH_PROCESS.dstreamDesc;
        }
    }
}
