package com.ymatou.mq.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * rabbit生产者
 * Created by zhangzhihua on 2017/3/23.
 */
public class RabbitProducer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitProducer.class);

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
     * 当前rabbit集群，默认master
     */
    private String cluster = CLUSTER_MASTER;

    /**
     * rabbit集群-master
     */
    private static final String CLUSTER_MASTER = "master";

    /**
     * rabbit集群-slave
     */
    private static final String CLUSTER_SLAVE = "slave";

    /**
     * master通道
     */
    private Channel masterChannel;

    /**
     * slave通道
     */
    private Channel slaveChannel;

    /**
     * 默认一个连接创建通道数目
     */
    private static final int CHANNEL_NUMBER = 10;

    public RabbitProducer(String appId,String bizCode){
        this.exchange = appId;
        this.routingKey = bizCode;
        //TODO 确认
        this.queue = bizCode;
        //初始化创建channel
        this.initChannel();
    }

    /**
     * 创建通道
     */
    void initChannel(){
        try {
            //通过master集群创建
            this.masterChannel = this.createChannel(CLUSTER_MASTER);
            this.cluster = CLUSTER_MASTER;
        } catch (Exception e) {
            logger.error("create master conn failed,try slave...",e);
            try {
                //若master不可用，则通过slave创建
                this.slaveChannel = this.createChannel(CLUSTER_SLAVE);
                this.cluster = CLUSTER_SLAVE;
            } catch (Exception ex) {
                logger.error("create slave conn failed.",e);
                throw new RuntimeException("create rabbit conn failed");
            }
        }
    }

    /**
     * 创建生产通道
     * @return
     */
    Channel createChannel(String cluster){
        try {
            //创建conn
            Connection conn = this.createConnection(cluster);
            //创建channel
            Channel channel = conn.createChannel(CHANNEL_NUMBER);
            channel.exchangeDeclare(exchange, "direct", true);
            channel.queueDeclare(queue, true, false, false, null);
            channel.queueBind(queue, exchange, queue);
            channel.basicQos(1);
            return channel;
        } catch (Exception e) {
            throw new RuntimeException("create rabbit channel failed.",e);
        }
    }

    /**
     * 创建conn
     * @return
     */
    Connection createConnection(String cluster){
        //创建conn
        Connection conn = null;
        try {
            conn = RabbitConnectionFactory.createConnection(cluster);
        } catch (Exception e) {
            throw new RuntimeException("create rabbit conn failed:" + e);
        }
        if(conn == null){
            throw new RuntimeException("create rabbit conn failed.");
        }
        return conn;
    }

    /**
     * 发布消息
     * @param
     * @param clientMsgId
     * @param msgId @throws IOException
     */
    public void publish(String body, String clientMsgId, String msgId) throws IOException {
        //若因通道连接发布失败，则切换集群连接重试
        try {
            AMQP.BasicProperties basicProps = new AMQP.BasicProperties.Builder()
                    .messageId(clientMsgId).correlationId(msgId)
                    .type(CLUSTER_MASTER)
                    .build();
            //import org.apache.commons.lang.SerializationUtils;
            //TODO convert string to bytes
            byte[] bd = null;
            masterChannel.basicPublish(exchange, routingKey, basicProps, bd);
        } catch (IOException e) {
            logger.error("publish master msg error,msgId:{},msgUuid:{}", clientMsgId, msgId,e);
            AMQP.BasicProperties basicProps = new AMQP.BasicProperties.Builder()
                    .messageId(clientMsgId).correlationId(msgId)
                    .type(CLUSTER_SLAVE)
                    .build();
            slaveChannel.basicPublish(exchange, routingKey, basicProps, null);
        }
    }
}
