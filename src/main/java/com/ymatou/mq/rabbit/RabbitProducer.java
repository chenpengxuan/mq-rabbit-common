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
     * 队列名称
     */
    private String queue = null;

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
        this.queue = queue;
        //TODO props init
        this.init();
    }

    /**
     * 初始化conn/channel相关
     */
    void init(){
        try {
            //初始化时创建conn/channel
            Connection conn = RabbitConnectionFactory.createConnection();
            this.channel = this.createChannel(conn);
        } catch (Exception e) {
            throw new RuntimeException("init conn/channel error.",e);
        }
    }

    /**
     * 创建通道
     * @param connection
     * @return
     */
    Channel createChannel(Connection connection) throws IOException {
        Channel channel = connection.createChannel(CHANNEL_NUMBER);
        channel.exchangeDeclare(exchange, "direct", true);
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, exchange, queue);
        channel.basicQos(1);
        return channel;
    }

    /**
     * 发布消息
     * @param
     * @throws IOException
     */
    public void publish(byte[] body) throws IOException {
        //TODO 调用时或心跳检测时，根据可用状态切换conn/channel
        //TODO convert string to bytes
        channel.basicPublish(exchange, routingKey, props, body);
    }
}
