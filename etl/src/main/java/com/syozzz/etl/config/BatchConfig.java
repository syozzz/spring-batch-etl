package com.syozzz.etl.config;

import cn.hutool.core.lang.Assert;
import com.syozzz.etl.builder.DataSourceBuilder;
import com.syozzz.etl.builder.ExecutorBuilder;
import com.syozzz.etl.builder.SqlBuilder;
import com.syozzz.etl.builder.SqlType;
import com.syozzz.etl.entity.BaseBatchProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.Map;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "syozzz.etl.enable", havingValue = "true")
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    SqlBuilder sqlBuilder;

    @Autowired
    DataSourceBuilder dataSourceBuilder;

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

    @Bean
    @JobScope
    Step taskStep(@Qualifier("commonJdbcItemReader") @StepScope ItemReader<Map<String, Object>> commonJdbcItemReader,
                  @Qualifier("commonJdbcItemWritter") @StepScope ItemWriter<Map<String, Object>> commonJdbcItemWritter,
                  BaseBatchProperties baseBatchProperties) {
        return steps.get("taskStep")
                .<Map<String, Object>, Map<String, Object>>chunk(baseBatchProperties.getChunkSize())
                .reader(commonJdbcItemReader)
                .writer(commonJdbcItemWritter)
                .build();
    }

    //通用 ItemReader
    @Bean
    @StepScope
    ItemReader<Map<String, Object>> commonJdbcItemReader(@Value("#{jobParameters}") Map<String, Object> jobParameters,
                                                     BaseBatchProperties baseBatchProperties) throws Exception {
        JdbcCursorItemReader<Map<String, Object>> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSourceBuilder.get());
        reader.setName("commonJdbcItemReader");
        reader.setFetchSize(baseBatchProperties.getFetchSize());
        reader.setSql(sqlBuilder.generateSql(SqlType.SELECT, jobParameters));
        reader.setRowMapper(new ColumnMapRowMapper());
        return reader;
    }

    //通用 ItemWritter
    @Bean
    @StepScope
    ItemWriter<Map<String, Object>> commonJdbcItemWritter(@Value("#{jobParameters}") Map<String, Object> jobParameters) throws Exception {
        JdbcBatchItemWriter<Map<String, Object>> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSourceBuilder.get());
        writer.setSql(sqlBuilder.generateSql(SqlType.INSERT, jobParameters));
        //因为处理的 item 是 map，所以无需设置 ItemSqlParameterSourceProvider
        return writer;
    }

}
