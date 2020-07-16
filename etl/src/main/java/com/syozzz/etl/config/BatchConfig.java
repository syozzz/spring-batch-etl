package com.syozzz.etl.config;

import cn.hutool.core.lang.Assert;
import com.syozzz.etl.builder.ExecutorBuilder;
import com.syozzz.etl.entity.BaseBatchProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.Resource;
import javax.sql.DataSource;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "syozzz.etl.enable", havingValue = "true")
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private StepBuilderFactory steps;

    @Resource(
            name = "${syozzz.etl.datasource:etldb}"
    )
    private DataSource dataSource;

    @Bean
    @ConfigurationProperties(prefix = "syozzz.etl")
    public BaseBatchProperties zxcpBatchProperties() {
        return new BaseBatchProperties();
    }

    @Bean("batchTaskExecutor")
    public ThreadPoolTaskExecutor taskBatchExecutor(BaseBatchProperties baseBatchProperties) {
        return ExecutorBuilder.build(baseBatchProperties.getTaskThreadNamePrefix(),
                baseBatchProperties.getTaskCorePoolSize(),
                baseBatchProperties.getTaskMaxPoolSize());
    }

    @Bean("batchStepExecutor")
    public ThreadPoolTaskExecutor stepBatchExecutor(BaseBatchProperties baseBatchProperties) {
        return ExecutorBuilder.build(baseBatchProperties.getStepThreadNamePrefix(),
                baseBatchProperties.getStepCorePoolSize(),
                baseBatchProperties.getStepMaxPoolSize());
    }

    @Bean
    public BatchConfigurer batchConfigurer(@Qualifier("batchTaskExecutor") final ThreadPoolTaskExecutor executor,
                                           @Value("${syozzz.etl.datasource:etldb}") String dataSourceName) {

        Assert.notNull(dataSource, "datasource[{}]注入失败, 请配置[{}]指定 batch 注册及配置使用的数据源", dataSourceName, "syozzz.etl.datasource");

        log.info("使用数据源[{}]创建 jobRepository...", dataSourceName);

        return new DefaultBatchConfigurer(dataSource) {

            @Override
            protected JobLauncher createJobLauncher() throws Exception {
                SimpleJobLauncher launcher = (SimpleJobLauncher) super.createJobLauncher();
                launcher.setTaskExecutor(executor);
                return launcher;
            }

            @Override
            protected JobRepository createJobRepository() throws Exception {
                JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
                factory.setDataSource(dataSource);
                factory.setIsolationLevelForCreate("ISOLATION_READ_COMMITTED");
                factory.setTransactionManager(super.getTransactionManager());
                factory.afterPropertiesSet();
                return factory.getObject();
            }
        };
    }

}
