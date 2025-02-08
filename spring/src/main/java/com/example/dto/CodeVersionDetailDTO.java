package com.example.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Builder
public class CodeVersionDetailDTO {
    private String pipelineId;

    private Long pipelineVersion;

    private Long globalVersion;

    private String storageUrl;
}
