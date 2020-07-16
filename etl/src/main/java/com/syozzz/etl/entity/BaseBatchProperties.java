package com.syozzz.etl.entity;

import lombok.Data;

@Data
public class BaseBatchProperties {

    private String TaskThreadNamePrefix = "zxcp-batch-task-";
    private int taskCorePoolSize = 10;
    private int taskMaxPoolSize = 20;
    private String StepThreadNamePrefix = "zxcp-batch-step-";
    private int stepCorePoolSize = 10;
    private int stepMaxPoolSize = 20;

}
