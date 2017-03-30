package com.ymatou.mq.rabbit.support;

import com.rabbitmq.client.Channel;
import com.ymatou.mq.infrastructure.model.Message;

import java.io.IOException;
import java.util.SortedMap;

/**
 * rabbit ack处理接口
 * Created by zhangzhihua on 2017/3/30.
 */
public interface RabbitAckHandler {

    /**
     * 处理ack事件
     * @param deliveryTag
     * @param multiple
     */
    public  void handleAck(long deliveryTag, boolean multiple,Channel channel,SortedMap<Long, Message> unconfirmedSet);

    /**
     * 处理nack事件
     * @param deliveryTag
     * @param multiple
     * @throws IOException
     */
    public void handleNack(long deliveryTag, boolean multiple,Channel channel,SortedMap<Long, Message> unconfirmedSet) throws IOException;
}
