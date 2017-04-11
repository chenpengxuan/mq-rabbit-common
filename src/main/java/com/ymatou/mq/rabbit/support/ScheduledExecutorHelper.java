/*
 *
 * (C) Copyright 2017 Ymatou (http://www.ymatou.com/). All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.support;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * @author luoshiqian 2017/2/27 18:35
 */
public class ScheduledExecutorHelper {

    private static Map<String, ScheduledExecutorService> scheduledExecutorServiceMap = Maps.newConcurrentMap();

    /**
     * 关闭时 关闭线程池
     */
    static {
        Runtime.getRuntime().addShutdownHook(new Thread("ScheduledExecutorHelper|ShutdownHook") {
            @Override
            public void run() {
                shutdown();
            }
        });
    }

    /**
     * 关闭
     */
    public static synchronized void shutdown(){
        scheduledExecutorServiceMap.values().stream().forEach(ScheduledExecutorService::shutdownNow);
    }

    /**
     * 创建一个线程名称的 定时任务
     * 
     * @param threadName
     * @return
     */
    public static synchronized ScheduledExecutorService newSingleThreadScheduledExecutor(String threadName) {
        if (scheduledExecutorServiceMap.get(threadName) == null) {
            ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                    threadFactory(threadName));
            scheduledExecutorServiceMap.put(threadName,scheduledExecutorService);
            return scheduledExecutorService;
        }
        return scheduledExecutorServiceMap.get(threadName);
    }

    /**
     *
     * @param corePoolSize
     * @param threadName
     * @return
     */
    public static synchronized ScheduledExecutorService newScheduledThreadPool(int corePoolSize,String threadName) {
        if (scheduledExecutorServiceMap.get(threadName) == null) {
            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(corePoolSize,
                    threadFactory(threadName));
            return scheduledExecutorService;
        }
        return scheduledExecutorServiceMap.get(threadName);
    }

    /**
     * 设置线程工厂 线程名称 传入 rpc-pool format: rpc-pool-%d 生成 rpc-pool-1,rpc-pool-2
     * 
     * @param threadName
     * @return
     */
    public static ThreadFactory threadFactory(String threadName) {
        return new ThreadFactoryBuilder().setNameFormat(threadName + "-%d").build();
    }
}
