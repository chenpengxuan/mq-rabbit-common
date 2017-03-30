package com.ymatou.mq.rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.ymatou.mq.infrastructure.model.Message;
import com.ymatou.mq.rabbit.support.RabbitAckHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * rabbit ack监听扩展
 * Created by zhangzhihua on 2017/3/30.
 */
public class RabbitAckListener implements ConfirmListener {

    private static final Logger logger = LoggerFactory.getLogger(RabbitAckListener.class);

    /**
     * channel
     */
    private Channel channel;

    /**
     * 未确认集合
     */
    private SortedMap<Long, Message> unconfirmedSet;

    /**
     * rabbit ack实际回调处理
     */
    private RabbitAckHandler rabbitAckHandler;

    public RabbitAckListener(Channel channel, SortedMap<Long, Message> unconfirmedSet, RabbitAckHandler rabbitAckHandler){
        this.channel = channel;
        this.unconfirmedSet = unconfirmedSet;
        this.rabbitAckHandler = rabbitAckHandler;
        logger.debug("new RabbitAckListener,current thread name:{},thread id:{},channel:{},unconfirmedSet:{},rabbitAckHandler:{}",Thread.currentThread().getName(),Thread.currentThread().getId(),channel.hashCode(),unconfirmedSet,rabbitAckHandler);
    }

    @Override
    public void handleAck(long deliveryTag, boolean multiple) throws IOException {
        rabbitAckHandler.handleAck(deliveryTag,multiple,channel,unconfirmedSet);
    }

    @Override
    public void handleNack(long deliveryTag, boolean multiple) throws IOException {
        rabbitAckHandler.handleNack(deliveryTag,multiple,channel,unconfirmedSet);
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public SortedMap<Long, Message> getUnconfirmedSet() {
        return unconfirmedSet;
    }

    public void setUnconfirmedSet(SortedMap<Long, Message> unconfirmedSet) {
        this.unconfirmedSet = unconfirmedSet;
    }
}
