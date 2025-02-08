package com.example.controller;

import com.example.dto.CodeVersionDetailDTO;
import com.example.entity.CodeVersionDetailEntity;
import com.example.enums.DataProcessTypeEnum;
import com.example.enums.EngineTypeEnum;
import com.example.enums.GlobalTypeEnum;
import com.example.service.CodeVersionDetailService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/pipeline/condenser")
public class CodeVersionDetailController {
    @Autowired
    private CodeVersionDetailService codeVersionDetailService;


    @RequestMapping("/updateGlobalVersion")
    public void updateGlobalVersion() {
        codeVersionDetailService.updateGlobalVersion(0, 2);
    }

    /**
     * 获取引擎最新的globalVersion，每个引擎对应一个全局唯一的id
     * @return global version
     */
    @GetMapping("/getGlobalCodeVersion")
    public Long getGlobalCodeVersion(@RequestParam Integer globalType, @RequestParam Integer engine) {
        log.info("getGlobalCodeVersion params: globalType={}, engine={}", globalType, engine);
        return codeVersionDetailService.getGlobalVersion(globalType, engine);
    }

    /**
     * 获取condenser数据流全量代码信息
     * @return global version
     */
    @GetMapping("/getAllCodeInfo")
    public List<CodeVersionDetailDTO> getAllCodeInfo(@RequestParam Integer engine) {
        log.info("getAllCodeInfo params: engine={}", engine);
        return codeVersionDetailService.getAllCodeInfo(engine);
    }

    /**
     * 获取引擎两个globalVersion之间的代码信息
     *
     * @return global version
     */
    @GetMapping("/getIncCodeInfo")
    public List<CodeVersionDetailDTO> getIncCodeInfo(@RequestParam Integer engine, @RequestParam Long startVersion, @RequestParam Long endVersion) {
        return codeVersionDetailService.getIncCodeInfo(engine, startVersion, endVersion);
    }

    /**
     * 插入数据测试：
     */
    @GetMapping("/insertVersionDetail")
    public void insertVersionDetail(@RequestParam String pipelineId, @RequestParam Long pipelineVersion) {
        CodeVersionDetailEntity versionDetailEntity = CodeVersionDetailEntity.builder()
                .pipelineId(pipelineId)
                .pipelineVersion(pipelineVersion)
                .engine(EngineTypeEnum.CONDENSER.getCode())
                .isDeleted(0)
                .processType(DataProcessTypeEnum.BATCH_PROCESS.getType())
                .storageUrl("http://" + pipelineId)
                .build();

        codeVersionDetailService.insertCodeVersionDetail(GlobalTypeEnum.CODE_VERSION.getCode(), versionDetailEntity);
    }

    /**
     * 插入数据测试：
     */
    @GetMapping("/updateVersionDetail")
    public void updateVersionDetail(@RequestParam String pipelineId) {;
        codeVersionDetailService.updateCodeVersionDetail(1, pipelineId);
    }
}