package com.ymatou.mq.rabbit.support;

import com.rabbitmq.client.Connection;

/**
 * 连接wrapper
 * Created by zhangzhihua on 2017/3/28.
 */
public class ConnectionWrapper {

    private Connection connection;

    private int count;

    public ConnectionWrapper(Connection connection){
        this.connection = connection;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void incr(){
        count++;
    }

    public void decr(){
        count--;
    }
}
