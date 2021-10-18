package com.at.loggercreate;

import com.at.loggercreate.config.AppConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author zero
 * @create 2021-06-26 17:58
 */
@Component
public class MockTask {
//    @Autowired
//    ThreadPoolTaskExecutor poolExecutor;

    @Autowired
    Executor executor;


    public MockTask() {
    }

    public void mainTask() throws InterruptedException {
        for (int i = 0; i < AppConfig.mock_count; ++i) {


            Thread.sleep(AppConfig.send_delay);


//            this.poolExecutor.execute(new Mocker());

            CompletableFuture.runAsync(new Mocker(), executor);
            ((ThreadPoolExecutor) executor).shutdown();


            System.out.println("active+" + ((ThreadPoolExecutor) executor).getActiveCount());
        }

        while (true) {
            try {
                Thread.sleep(1000L);
                if (((ThreadPoolExecutor) executor).getActiveCount() == 0) {
//                    ((ThreadPoolExecutor) executor).shutdown();
                    return;
                }
            } catch (InterruptedException var2) {
                var2.printStackTrace();
            }
        }
    }
}
