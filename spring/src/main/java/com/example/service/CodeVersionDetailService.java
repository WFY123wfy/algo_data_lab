package com.example.service;

import com.example.dto.CodeVersionDetailDTO;
import com.example.entity.CodeVersionDetailEntity;
import com.example.repository.CodeVersionDetailRepository;
import com.example.repository.GlobalVersionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestParam;

import javax.persistence.LockModeType;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class CodeVersionDetailService {
    @Autowired
    private GlobalVersionRepository globalVersionRepository;

    @Autowired
    private CodeVersionDetailRepository codeVersionDetailRepository;

    /**
     * 更新全局版本号接口
     */
    @Transactional
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    public Long updateGlobalVersion(Integer globalType, Integer engine) {
        globalVersionRepository.updateGlobalVersion(globalType, engine);
        return globalVersionRepository.getGlobalVersion(globalType, engine);
    }

    public Long getGlobalVersion(Integer globalType, Integer engine) {
        return globalVersionRepository.getGlobalVersion(globalType, engine);
    }

    public List<CodeVersionDetailDTO> getAllCodeInfo(Integer engine) {
        List<CodeVersionDetailEntity> codeVerDetails = codeVersionDetailRepository.findByGlobalTypeAndEngine(engine);
        return codeVerDetails.stream()
                .map(entity -> CodeVersionDetailDTO.builder()
                        .pipelineId(entity.getPipelineId())
                        .pipelineVersion(entity.getPipelineVersion())
                        .globalVersion(entity.getGlobalVersion())
                        .storageUrl(entity.getStorageUrl())
                        .build()
                ).collect(Collectors.toList());
    }

    public List<CodeVersionDetailDTO> getIncCodeInfo(Integer engine, Long startVersion, Long endVersion) {
        List<CodeVersionDetailEntity> codeVerDetails = codeVersionDetailRepository.findByGlobalVersionBetween(engine, startVersion, endVersion);
        return codeVerDetails.stream()
                .map(entity -> CodeVersionDetailDTO.builder()
                    .pipelineId(entity.getPipelineId())
                    .pipelineVersion(entity.getPipelineVersion())
                    .globalVersion(entity.getGlobalVersion())
                    .storageUrl(entity.getStorageUrl())
                    .build()
                ).collect(Collectors.toList());
    }

    @Transactional
    public void insertCodeVersionDetail(Integer globalType, CodeVersionDetailEntity codeVersionDetailEntity) {
        Integer engine = codeVersionDetailEntity.getEngine();
        Long globalVersion = updateGlobalVersion(globalType, engine);
        codeVersionDetailEntity.setGlobalVersion(globalVersion);
        codeVersionDetailRepository.save(codeVersionDetailEntity);
    }

    public void updateCodeVersionDetail(Integer isDeleted, String pipelineId) {
        codeVersionDetailRepository.updateCodeVersionDetail(isDeleted, pipelineId);
    }
}
