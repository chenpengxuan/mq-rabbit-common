package com.ymatou.mq.rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * rabbit channel创建工厂
 * Created by zhangzhihua on 2017/3/24.
 */
public class RabbitChannelFactory {

    private static final Logger logger = LoggerFactory.getLogger(RabbitChannelFactory.class);

    /**
     * 默认一个连接创建通道数目
     */
    private static final int DEFAULT_CHANNEL_NUMBER = 20;

    /**
     * channel映射表
     */
    private static Map<String,Channel> channelMapping = new ConcurrentHashMap<String,Channel>();

    /**
     * 获取channel
     * @param cluster
     * @param queue
     * @return
     */
    public static Channel getChannel(String cluster, RabbitConfig rabbitConfig, String queue){
        String channelId = String.format("%s_%s",cluster,queue);
        if(channelMapping.get(channelId) != null){
            return channelMapping.get(channelId);
        }else{
            Channel channel = createChannel(cluster,rabbitConfig,queue);
            channelMapping.put(channelId,channel);
            return channel;
        }
    }

    /**
     * 创建生产通道
     * @return
     */
    static Channel createChannel(String cluster, RabbitConfig rabbitConfig, String queue){
        try {
            //创建conn
            Connection conn = getConnection(cluster,rabbitConfig);
            if(conn == null){
                throw new RuntimeException("create rabbit conn failed.");
            }
            //创建channel
            Channel channel = conn.createChannel(DEFAULT_CHANNEL_NUMBER);
            //channel.exchangeDeclare(exchange, "direct", true);
            channel.queueDeclare(queue, true, false, false, null);
            //channel.queueBind(queue, exchange, queue);
            channel.basicQos(1);
            return channel;
        } catch (Exception e) {
            throw new RuntimeException("create rabbit channel failed.",e);
        }
    }

    /**
     * 获取conn
     * @return
     */
    static Connection getConnection(String cluster, RabbitConfig rabbitConfig){
        try {
            //创建conn
            Connection conn = RabbitConnectionFactory.createConnection(cluster,rabbitConfig);
            return conn;
        } catch (Exception e) {
            throw new RuntimeException("create rabbit conn failed:" + e);
        }
    }

}
