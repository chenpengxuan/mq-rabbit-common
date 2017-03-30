package com.ymatou.mq.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.ymatou.mq.infrastructure.model.Message;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.ChannelWrapper;
import com.ymatou.mq.rabbit.support.RabbitAckHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

/**
 * rabbit生产者
 * Created by zhangzhihua on 2017/3/23.
 */
@Component("rabbitProducer")
public class RabbitProducer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitProducer.class);

    /**
     * rabbit配置信息
     */
    @Autowired
    private RabbitConfig rabbitConfig;

    /**
     * rabbit ack回调处理类
     */
    private RabbitAckHandler rabbitAckHandler;

    /**
     * 发布消息
     * @param queue
     * @param message
     * @throws IOException
     */
    public void publish(String queue, Message message) throws IOException {
        String msgId = message.getId();
        String bizId = message.getBizId();
        String body = message.getBody();

        //获取channel
        ChannelWrapper channelWrapper = RabbitChannelFactory.getChannelWrapper(rabbitConfig);
        Channel channel = channelWrapper.getChannel();
        //若是第一次创建channel，则初始化ack相关
        if(channelWrapper.getUnconfirmedSet() == null){
            //保证channel与unconfirmedset&acklistener一对一
            SortedMap<Long, Message> unconfirmedSet = Collections.synchronizedSortedMap(new TreeMap<Long, Message>());
            channelWrapper.setUnconfirmedSet(unconfirmedSet);
            RabbitAckListener rabbitAckListener = new RabbitAckListener(channel,unconfirmedSet,rabbitAckHandler);
            channel.addConfirmListener(rabbitAckListener);
            channel.confirmSelect();
        }
        //声明队列
        this.declareQueue(channel,queue);

        //设置ack关联数据
        channelWrapper.getUnconfirmedSet().put(channel.getNextPublishSeqNo(),message);

        //TODO basicProps persiste
        //MessageProperties.MINIMAL_PERSISTENT_BASIC
        AMQP.BasicProperties basicProps = new AMQP.BasicProperties.Builder()
                .messageId(bizId).correlationId(msgId)
                .type(rabbitConfig.getCurrentCluster())
                .build();
        channel.basicPublish("", queue, basicProps, body.getBytes());
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

    public void setRabbitAckHandler(RabbitAckHandler rabbitAckHandler) {
        this.rabbitAckHandler = rabbitAckHandler;
    }
}
