package com.ymatou.mq.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.ymatou.mq.infrastructure.model.Message;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.RabbitConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
     * rabbit ack事件监听
     */
    private ConfirmListener confirmListener;

    /**
     * 未确认集合
     */
    private SortedMap<Long, Message> unconfirmedSet = Collections.synchronizedSortedMap(new TreeMap<Long, Message>());

    /**
     * 发布消息
     * FIXME：参数就两个: queue, msg. rabbitConfig提取为属性?
     * @param queue
     * @param message
     *@param rabbitConfig  @throws IOException
     */
    public void publish(String queue, Message message, RabbitConfig rabbitConfig) throws IOException {
        String msgId = message.getId();
        String bizId = message.getBizId();
        String body = message.getBody();

        AMQP.BasicProperties basicProps = new AMQP.BasicProperties.Builder()
                .messageId(bizId).correlationId(msgId)
                .type(rabbitConfig.getCurrentCluster())
                .build();

        //获取channel
        Channel channel = RabbitChannelFactory.getChannel(rabbitConfig);
        //声明队列
        this.declareQueue(channel,queue);
        //设置confirm listener
        channel.addConfirmListener(this.getConfirmListener());
        channel.confirmSelect();
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

    public void setConfirmListener(ConfirmListener confirmListener) {
        this.confirmListener = confirmListener;
    }

    public ConfirmListener getConfirmListener() {
        return confirmListener;
    }

    public SortedMap<Long, Message> getUnconfirmedSet() {
        return unconfirmedSet;
    }

}
