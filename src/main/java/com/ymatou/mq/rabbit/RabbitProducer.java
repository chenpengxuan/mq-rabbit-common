package com.ymatou.mq.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.RabbitConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

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
    private String cluster = RabbitConstants.CLUSTER_MASTER;

    /**
     * 默认一个连接创建通道数目
     */
    private static final int CHANNEL_NUMBER = 10;

    /**
     * master通道
     */
    private Channel masterChannel;

    /**
     * slave通道
     */
    private Channel slaveChannel;

    /**
     * rabbit ack事件监听
     */
    private ConfirmListener confirmListener;

    /**
     * rabbit配置信息
     */
    private RabbitConfig rabbitConfig;

    public RabbitProducer(String appId, String bizCode, ConfirmListener confirmListener, RabbitConfig rabbitConfig){
        this.exchange = appId;
        this.routingKey = bizCode;
        //TODO 确认
        this.queue = bizCode;
        this.confirmListener = confirmListener;
        this.rabbitConfig = rabbitConfig;
        //初始化channel相关
        this.init();
    }

    /**
     * 初始化channel相关
     */
    void init(){
        try {
            if(this.isMasterEnable(this.rabbitConfig)){
                this.masterChannel = this.createChannel(RabbitConstants.CLUSTER_MASTER);
            }
            if(this.isSlaveEnable(this.rabbitConfig)){
                this.slaveChannel = this.createChannel(RabbitConstants.CLUSTER_SLAVE);
            }
        } catch (Exception e) {
            throw new RuntimeException("create rabbit conn failed",e);
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
            if(channel != null){
                channel.addConfirmListener(this.confirmListener);
            }
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
            conn = RabbitConnectionFactory.createConnection(cluster,this.rabbitConfig);
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
     * @param body
     * @param clientMsgId
     * @param msgId
     * @param rabbitConfig
     * @throws IOException
     */
    public void publish(String body, String clientMsgId, String msgId, RabbitConfig rabbitConfig) throws IOException {
        //若master/slave都没有开启
        if(!this.isMasterEnable(rabbitConfig) && !this.isSlaveEnable(rabbitConfig)){
            throw new RuntimeException("master and slave not enable.");
        }else if(this.isMasterEnable(rabbitConfig)){//若master开启
            AMQP.BasicProperties basicProps = new AMQP.BasicProperties.Builder()
                    .messageId(clientMsgId).correlationId(msgId)
                    .type(RabbitConstants.CLUSTER_MASTER)
                    .build();
            this.cluster = RabbitConstants.CLUSTER_MASTER;
            //import org.apache.commons.lang.SerializationUtils;
            //TODO convert string to bytes
            byte[] bd = null;
            this.getMasterChannel().basicPublish(exchange, routingKey, basicProps, bd);
        }else if(this.isSlaveEnable(rabbitConfig)){//若slave开启
            AMQP.BasicProperties basicProps = new AMQP.BasicProperties.Builder()
                    .messageId(clientMsgId).correlationId(msgId)
                    .type(RabbitConstants.CLUSTER_SLAVE)
                    .build();
            this.cluster = RabbitConstants.CLUSTER_SLAVE;
            this.getSlaveChannel().basicPublish(exchange, routingKey, basicProps, null);
        }
    }

    /**
     * 获取master channel
     * @return
     */
    public Channel getMasterChannel() {
        if(masterChannel == null){
            masterChannel = this.createChannel(RabbitConstants.CLUSTER_MASTER);
            if(masterChannel == null){
                throw new RuntimeException("create master channel fail.");
            }
        }
        return masterChannel;
    }

    /**
     * 获取slave channel
     * @return
     */
    public Channel getSlaveChannel() {
        if(slaveChannel == null){
            slaveChannel = this.createChannel(RabbitConstants.CLUSTER_SLAVE);
            if(slaveChannel == null){
                throw new RuntimeException("create slave channel fail.");
            }
        }
        return slaveChannel;
    }

    /**
     * 判断master是否开启
     * @param rabbitConfig
     * @return
     */
    boolean isMasterEnable(RabbitConfig rabbitConfig){
        if(rabbitConfig.getMasterEnable() == 1){
            return true;
        }
        return false;
    }

    /**
     * 判断slave是否开启
     * @param rabbitConfig
     * @return
     */
    boolean isSlaveEnable(RabbitConfig rabbitConfig){
        if(rabbitConfig.getSlaveEnable() == 1){
            return true;
        }
        return false;
    }
}
