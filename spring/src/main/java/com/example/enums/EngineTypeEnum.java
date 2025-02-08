package com.example.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum EngineTypeEnum {
    SPARK(0,"Spark"),
    FLINK(1,"Flink"),
    CONDENSER(2,"Condenser");
    private final Integer code;
    private final String desc;
}
