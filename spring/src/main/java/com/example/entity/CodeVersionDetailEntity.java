package com.example.entity;

import com.example.enums.DataProcessTypeEnum;
import com.example.enums.EngineTypeEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import java.util.Date;

@Table(name="data_stream_code_version_detail", uniqueConstraints = {@UniqueConstraint(columnNames={"pipeline_id", "engine", "global_version"})})
@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CodeVersionDetailEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "pipeline_id", nullable = false, updatable = false)
    private String pipelineId;

    @Column(name = "pipeline_version")
    private Long pipelineVersion = 0L;

    @Column(name = "global_version", nullable = false)
    private Long globalVersion;

    /**
     * @see EngineTypeEnum
     */
    @Column(nullable = false)
    private Integer engine = 0;

    /**
     * @see DataProcessTypeEnum
     */
    @Column(name = "process_type")
    private Integer processType = 1;

    /**
     * 0: 未删除, 1: 已删除
     */
    @Column(name = "is_deleted")
    private Integer isDeleted = 0;

    @Column(name = "storage_url")
    private String storageUrl;

    @CreationTimestamp
    @Column(name = "create_time", nullable = false, updatable = false)
    private Date createTime;

    @UpdateTimestamp
    @Column(name = "modify_time", nullable = false)
    private Date modifyTime;
}