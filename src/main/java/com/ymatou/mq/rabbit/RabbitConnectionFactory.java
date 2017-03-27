package com.ymatou.mq.rabbit;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

/**
 * rabbit连接工厂，主从url要透明化
 * Created by zhangzhihua on 2017/3/27.
 */
public class RabbitConnectionFactory {

    private String masterUrl;

    private String slaveUrl;

    /**
     * 创建连接 TODO 连接复用
     * @return
     */
    public static Connection createConnection() throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        ConnectionFactory connectionFactory = createConnectionFactory();
        return connectionFactory.newConnection();
    }

    /**
     * 创建连接工厂
     * @param uri
     */
    static ConnectionFactory createConnectionFactory() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        ConnectionFactory factory = new ConnectionFactory();
        //TODO set url
        factory.setUri("");
        factory.setAutomaticRecoveryEnabled(true);
        //TODO 心跳检测 ScheduledExecutorService
        factory.setHeartbeatExecutor(null);
        return factory;
    }
}
