package com.example.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum GlobalTypeEnum {
    CODE_VERSION(0,"code_version");

    private final Integer code;

    private final String desc;
}
