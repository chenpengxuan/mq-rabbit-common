package com.ymatou.mq.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.RabbitConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * rabbit生产者
 * Created by zhangzhihua on 2017/3/23.
 */
public class RabbitProducer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitProducer.class);

    /**
     * 当前rabbit集群，默认master
     */
    private String cluster = RabbitConstants.CLUSTER_MASTER;

    /**
     * rabbit配置信息
     */
    private RabbitConfig rabbitConfig;

    /**
     * rabbit ack事件监听
     */
    private ConfirmListener confirmListener;

    /**
     * 线程channel上下文
     */
    private ThreadLocal<Channel> channelHolder = new ThreadLocal<Channel>();

        /**
     * 未确认集合
     */
    private SortedMap<Long, Map<String,Object>> unconfirmedSet = Collections.synchronizedSortedMap(new TreeMap<Long, Map<String,Object>>());

    public RabbitProducer(RabbitConfig rabbitConfig, ConfirmListener confirmListener){
        this.rabbitConfig = rabbitConfig;
        this.confirmListener = confirmListener;
    }

    /**
     * 发布消息
     * FIXME：参数就两个: queue, msg. rabbitConfig提取为属性?
     * @param queue
     * @param body
     * @param clientMsgId
     * @param msgId
     * @param rabbitConfig
     * @throws IOException
     */
    public void publish(String queue, String body, String clientMsgId, String msgId, RabbitConfig rabbitConfig) throws IOException {
        if(!this.isMasterEnable(rabbitConfig) && !this.isSlaveEnable(rabbitConfig)){//若master/slave都没有开启
            throw new RuntimeException("master and slave not enable.");
        }else if(this.isMasterEnable(rabbitConfig)){//若master开启
            AMQP.BasicProperties basicProps = new AMQP.BasicProperties.Builder()
                    .messageId(clientMsgId).correlationId(msgId)
                    .type(RabbitConstants.CLUSTER_MASTER)
                    .build();
            this.cluster = RabbitConstants.CLUSTER_MASTER;

            Channel channel = this.getChannel(RabbitConstants.CLUSTER_MASTER, rabbitConfig);
            //设置confirm listener
            channel.addConfirmListener(confirmListener);
            channel.confirmSelect();
            //声明队列
            this.declareQueue(channel,queue);
            //设置ack关联数据
            unconfirmedSet.put(channel.getNextPublishSeqNo(),this.getPublishMessage(body,clientMsgId,msgId, queue));

            channel.basicPublish("", queue, basicProps, body.getBytes());
        }else if(this.isSlaveEnable(rabbitConfig)){//若slave开启
            AMQP.BasicProperties basicProps = new AMQP.BasicProperties.Builder()
                    .messageId(clientMsgId).correlationId(msgId)
                    .type(RabbitConstants.CLUSTER_SLAVE)
                    .build();
            this.cluster = RabbitConstants.CLUSTER_SLAVE;

            Channel channel = this.getChannel(RabbitConstants.CLUSTER_SLAVE, rabbitConfig);
            //设置confirm listener
            channel.addConfirmListener(confirmListener);
            channel.confirmSelect();
            //声明队列
            this.declareQueue(channel,queue);
            //设置ack关联数据
            unconfirmedSet.put(channel.getNextPublishSeqNo(),this.getPublishMessage(body,clientMsgId,msgId, queue));

            channel.basicPublish("", queue, basicProps, body.getBytes());
        }
    }

    /**
     * 获取要发布的消息
     * @param body
     * @param bizId
     * @param msgId
     * @param queue
     * @return
     */
    Map<String,Object> getPublishMessage(String body, String bizId, String msgId, String queue){
        Map<String,Object> map = new HashMap<String,Object>();
        map.put(RabbitConstants.QUEUE_CODE,queue);
        map.put(RabbitConstants.MSG_ID,msgId);
        map.put(RabbitConstants.BIZ_ID, bizId);
        map.put(RabbitConstants.BODY,body);
        return map;
    }

    /**
     * 获取channel
     * @return
     */
    Channel getChannel(String cluster, RabbitConfig rabbitConfig) {
        Channel channel = channelHolder.get();
        if(channel != null){
            return channel;
        }else{
            channel = RabbitChannelFactory.createChannel(cluster,rabbitConfig);
            if(channel == null){
                throw new RuntimeException("create channel fail.");
            }
            channelHolder.set(channel);
            return channel;
        }
    }

    /**
     * 声明队列
     * @param channel
     * @param queue
     * @throws IOException
     */
    void declareQueue(Channel channel,String queue) throws IOException {
        //channel.exchangeDeclare(exchange, "direct", true);
        channel.queueDeclare(queue, true, false, false, null);
        //channel.queueBind(queue, exchange, queue);
        channel.basicQos(1);
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

    public ConfirmListener getConfirmListener() {
        return confirmListener;
    }

    public SortedMap<Long, Map<String, Object>> getUnconfirmedSet() {
        return unconfirmedSet;
    }

}
