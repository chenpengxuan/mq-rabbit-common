package com.ymatou.mq.rabbit.support;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;

/**
 * channel wrapper，主要用于处理ack事件
 * Created by zhangzhihua on 2017/3/30.
 */
public class ChannelWrapper {

    private static final Logger logger = LoggerFactory.getLogger(ChannelWrapper.class);

    /**
     * channel
     */
    private Channel channel;

    /**
     * 未确认集合
     */
    private SortedMap<Long, Object> unconfirmedMap;

    /**
     * 当前channel所在线程
     */
    private Thread thread;

    /**
     * 当前channel所属connection wrapper
     */
    private ConnectionWrapper connectionWrapper;

    public ChannelWrapper(Channel channel){
        this.channel = channel;
    }

    public void addRecoveryListener(){
        if(this.getChannel() instanceof AutorecoveringChannel){
            AutorecoveringChannel recoverChannel = (AutorecoveringChannel)this.getChannel();
            recoverChannel.addRecoveryListener(new RecoveryListener() {
                @Override
                public void handleRecovery(Recoverable recoverable) {
                    logger.warn("channel handleRecovery,recoverable:{}",recoverable);
                    if(unconfirmedMap != null){
                        unconfirmedMap.clear();
                    }
                }

                @Override
                public void handleRecoveryStarted(Recoverable recoverable) {
                    logger.warn("channel handleRecoveryStarted,recoverable:{}",recoverable);
                }
            });
        }
    }

    public ChannelWrapper(Channel channel,SortedMap<Long, Object> unconfirmedMap){
        this.channel = channel;
        this.unconfirmedMap = unconfirmedMap;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public SortedMap<Long, Object> getUnconfirmedMap() {
        return unconfirmedMap;
    }

    public void setUnconfirmedMap(SortedMap<Long, Object> unconfirmedMap) {
        this.unconfirmedMap = unconfirmedMap;
    }

    public Thread getThread() {
        return thread;
    }

    public void setThread(Thread thread) {
        this.thread = thread;
    }

    public ConnectionWrapper getConnectionWrapper() {
        return connectionWrapper;
    }

    public void setConnectionWrapper(ConnectionWrapper connectionWrapper) {
        this.connectionWrapper = connectionWrapper;
    }
}
