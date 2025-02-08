package com.example.repository;

import com.example.entity.CodeVersionDetailEntity;
import com.example.entity.GlobalVersionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface CodeVersionDetailRepository extends JpaRepository<CodeVersionDetailEntity, Long>, JpaSpecificationExecutor<CodeVersionDetailEntity> {

    @Modifying
    @Transactional
    @Query(value = "update data_stream_code_version_detail set is_deleted = ?1 where pipeline_id = ?2", nativeQuery = true)
    void updateCodeVersionDetail(Integer isDeleted, String pipelineId);

    @Query(value = "SELECT t1.* " +
            " FROM data_stream_code_version_detail t1" +
            " JOIN (" +
            "     SELECT pipeline_id, MAX(pipeline_version) AS pipeline_version " +
            "     FROM data_stream_code_version_detail" +
            "     WHERE engine = ?1 " +
            "     GROUP BY pipeline_id " +
            " ) t2 " +
            " ON t1.pipeline_id = t2.pipeline_id AND t1.pipeline_version = t2.pipeline_version " +
            " WHERE engine = ?1", nativeQuery = true)
    List<CodeVersionDetailEntity> findByGlobalTypeAndEngine(Integer engine);

    @Query(value = "SELECT t1.* " +
            " FROM data_stream_code_version_detail t1" +
            " JOIN (" +
            "     SELECT pipeline_id, MAX(pipeline_version) AS pipeline_version " +
            "     FROM data_stream_code_version_detail" +
            "     WHERE engine = ?1 and global_version >= ?2 AND global_version <= ?3 " +
            "     GROUP BY pipeline_id " +
            " ) t2 " +
            " ON t1.pipeline_id = t2.pipeline_id AND t1.pipeline_version = t2.pipeline_version " +
            " WHERE engine = ?1 and global_version >= ?2 AND global_version <= ?3", nativeQuery = true)
    List<CodeVersionDetailEntity> findByGlobalVersionBetween(Integer engine, Long startVersion, Long endVersion);
}