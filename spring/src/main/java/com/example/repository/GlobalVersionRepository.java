package com.example.repository;

import com.example.entity.GlobalVersionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface GlobalVersionRepository extends JpaRepository<GlobalVersionEntity, Long>, JpaSpecificationExecutor<GlobalVersionEntity> {

    /**
     * 更新引擎的全局版本号
     *
     * @param globalType
     * @param engine
     */
    @Modifying
    @Transactional
    @Query(value = "update data_stream_global_version set global_version = (global_version + 1) where global_type=?1 and engine=?2", nativeQuery = true)
    void updateGlobalVersion(Integer globalType, Integer engine);

    @Query(value = "select global_version from data_stream_global_version where global_type=?1 and engine=?2", nativeQuery = true)
    Long getGlobalVersion(Integer globalType, Integer engine);
}