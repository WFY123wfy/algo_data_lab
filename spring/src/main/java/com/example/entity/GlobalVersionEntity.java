package com.example.entity;

import com.example.enums.DataProcessTypeEnum;
import com.example.enums.EngineTypeEnum;
import com.example.enums.GlobalTypeEnum;
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

@Table(name="data_stream_global_version", uniqueConstraints = {@UniqueConstraint(columnNames={"global_type", "engine", "process_type"})})
@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GlobalVersionEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * @see GlobalTypeEnum
     */
    @Column(name = "global_type", nullable = false)
    private Integer globalType = 0;

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

    @Column(name = "global_version", nullable = false)
    private Long globalVersion;

    @CreationTimestamp
    @Column(name = "create_time", nullable = false, updatable = false)
    private Date createTime;

    @UpdateTimestamp
    @Column(name = "modify_time", nullable = false)
    private Date modifyTime;
}