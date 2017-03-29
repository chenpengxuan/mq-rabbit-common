package com.ymatou.mq.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.ymatou.mq.infrastructure.model.Message;
import com.ymatou.mq.rabbit.config.RabbitConfig;
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
     * rabbit ack事件监听
     */
    private ConfirmListener confirmListener;

    /**
     * 未确认集合
     */
    private SortedMap<Long, Message> unconfirmedSet;

    /**
     * 发布消息
     * FIXME：参数就两个: queue, msg. rabbitConfig提取为属性?*
     * @param queue
     * @param message
     * @throws IOException
     */
    public void publish(String queue, Message message) throws IOException {
        String msgId = message.getId();
        String bizId = message.getBizId();
        String body = message.getBody();
        AMQP.BasicProperties basicProps = new AMQP.BasicProperties.Builder()
                .messageId(bizId).correlationId(msgId)
                .type(rabbitConfig.getCurrentCluster())
                .build();

        Channel channel = RabbitChannelFactory.getChannel(rabbitConfig);
        //设置confirm listener
        channel.addConfirmListener(confirmListener);
        channel.confirmSelect();
        //声明队列
        this.declareQueue(channel,queue);
        //设置ack关联数据
        unconfirmedSet.put(channel.getNextPublishSeqNo(),message);

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

    public ConfirmListener getConfirmListener() {
        return confirmListener;
    }

    public void setConfirmListener(ConfirmListener confirmListener) {
        this.confirmListener = confirmListener;
    }

    public SortedMap<Long, Message> getUnconfirmedSet() {
        return unconfirmedSet;
    }

    public void setUnconfirmedSet(SortedMap<Long, Message> unconfirmedSet) {
        this.unconfirmedSet = unconfirmedSet;
    }
}
