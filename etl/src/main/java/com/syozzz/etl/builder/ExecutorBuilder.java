package com.syozzz.etl.builder;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

public class ExecutorBuilder {

    public static ThreadPoolTaskExecutor build(String threadNamePrefix, int core, int max) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(core);
        executor.setMaxPoolSize(max);
        executor.setQueueCapacity(25);
        executor.setKeepAliveSeconds(200);
        executor.setThreadNamePrefix(threadNamePrefix);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        //额外的应用停机设置
        //等待所有任务都完成再继续销毁其他的Bean
        executor.setWaitForTasksToCompleteOnShutdown(true);
        //线程池中任务的等待时间，如果超过这个时候还没有销毁就强制销毁，以确保应用最后能够被关闭，而不是阻塞住
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();
        return executor;
    }

    public static ThreadPoolTaskExecutor build(String threadNamePrefix) {
        return build(threadNamePrefix, 10, 20);
    }

}
