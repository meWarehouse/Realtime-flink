package com.at.loggercreate.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.*;

/**
 * @author zero
 * @create 2021-06-26 17:38
 */
@Configuration
public class PoolConfig {
    public PoolConfig() {
    }

//    @Bean
//    public ThreadPoolTaskExecutor getPoolExecutor() {
//        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
//        threadPoolTaskExecutor.setCorePoolSize(10);
//        threadPoolTaskExecutor.setQueueCapacity(10000);
//        threadPoolTaskExecutor.setMaxPoolSize(1);
//        threadPoolTaskExecutor.initialize();
//        return threadPoolTaskExecutor;
//    }

    @Bean
    public Executor getPool(){
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
              10,
                30,
                2,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(2000),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.DiscardOldestPolicy()
        );

        return threadPoolExecutor;
    }


}
