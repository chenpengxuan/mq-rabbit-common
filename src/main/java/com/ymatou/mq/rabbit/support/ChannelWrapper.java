package com.ymatou.mq.rabbit.support;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.ymatou.mq.infrastructure.model.Message;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * channel wrapper，主要用于处理ack事件
 * Created by zhangzhihua on 2017/3/30.
 */
public class ChannelWrapper {

    /**
     * channel
     */
    private Channel channel;

    /**
     * 未确认集合
     */
    private SortedMap<Long, Message> unconfirmedSet;

    public ChannelWrapper(Channel channel){
        this.channel = channel;
    }

    public ChannelWrapper(Channel channel,SortedMap<Long, Message> unconfirmedSet){
        this.channel = channel;
        this.unconfirmedSet = unconfirmedSet;
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
