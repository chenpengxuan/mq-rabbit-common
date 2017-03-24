package com.ymatou.mq.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

/**
 * rabbit生产者
 * Created by zhangzhihua on 2017/3/23.
 */
public class RabbitProducer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitProducer.class);

    /**
     * 连接工厂
     */
    private ConnectionFactory connectionFactory;

    /**
     * 连接
     */
    private Connection connection;

    /**
     * 通道
     */
    private Channel channel;

    /**
     * 交换器名称
     */
    private String exchange = null;

    /**
     * 路由KEY
     */
    private String routingKey = null;

    /**
     * 连接属性
     */
    private AMQP.BasicProperties props = null;

    /**
     * 默认一个连接创建通道数目
     */
    private static final int CHANNEL_NUMBER = 10;

    public RabbitProducer(String appId,String bizCode){
        this.exchange = appId;
        this.routingKey = bizCode;
        //TODO props init
        this.init();
    }

    /**
     * 初始化conn/channel相关
     */
    void init(){
        try {
            //TODO 梳理appId/bizCode与url/address之间的关系
            //TODO 梳理connFactory/connection/channel之间关系
            ConnectionFactory connectionFactory = this.createConnectionFactory(null);
            Connection conn = this.createConnection(connectionFactory);
            Channel channel = this.createChannel(conn);
        } catch (Exception e) {
            throw new RuntimeException("init conn/channel error.",e);
        }
    }

    /**
     * 根据uri创建连接工厂
     * @param uri
     */
    ConnectionFactory createConnectionFactory(String uri) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(uri);
        factory.setAutomaticRecoveryEnabled(true);
        //TODO 心跳检测 ScheduledExecutorService
        factory.setHeartbeatExecutor(null);
        return factory;
    }

    /**
     * 创建连接
     * @return
     */
    Connection createConnection(ConnectionFactory connectionFactory) throws IOException, TimeoutException {
        return connectionFactory.newConnection();
    }

    /**
     * 创建通道
     * @param connection
     * @return
     */
    Channel createChannel(Connection connection) throws IOException {
        return connection.createChannel(CHANNEL_NUMBER);
    }

    /**
     * 发布消息
     * @param
     * @throws IOException
     */
    public void publish(byte[] body) throws IOException {
        //TODO convert string to bytes
        channel.basicPublish(exchange, routingKey, props, body);
    }
}
