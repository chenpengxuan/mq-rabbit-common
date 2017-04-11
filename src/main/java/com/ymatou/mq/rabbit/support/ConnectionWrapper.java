package com.ymatou.mq.rabbit.support;

import com.rabbitmq.client.Connection;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * FIXME: 基于count实现Comparable
 * 连接wrapper，主要用于计channel数目
 * Created by zhangzhihua on 2017/3/28.
 */
public class ConnectionWrapper {

    private Connection connection;

    //FIXME: rename with channelCount
    /**
     * 该conn下channel数目
     */
    private AtomicInteger count = new AtomicInteger(0);

    public ConnectionWrapper(Connection connection){
        this.connection = connection;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    /**
     * 获取连接使用数
     *
     * @return
     */
    public int getCount() {
        return count.intValue();
    }

    /**
     * 计数+1
     */
    public int incCount() {
        return count.incrementAndGet();
    }

    /**
     * 计数-1
     * @return
     */
    public int decCount() {
        return count.decrementAndGet();
    }

}
