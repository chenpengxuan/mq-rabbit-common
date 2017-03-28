package com.ymatou.mq.rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.ConnectionWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * rabbit channel创建工厂
 * Created by zhangzhihua on 2017/3/24.
 */
public class RabbitChannelFactory {

    private static final Logger logger = LoggerFactory.getLogger(RabbitChannelFactory.class);

    /**
     * 一个连接默认创建通道数目
     */
    private static final int DEFAULT_CHANNEL_NUMBER = 20;

    /**
     * 一个conn允许创建最大channel数目
     */
    private static final int MAX_CHANNEL_NUM = 50;


    /**
     * channel映射表
     */
    private static Map<String,Channel> channelMapping = new ConcurrentHashMap<String,Channel>();

    /**
     * connection wrapper映射表
     */
    private static Map<String,List<ConnectionWrapper>> connectionWrapperMapping = new ConcurrentHashMap<String,List<ConnectionWrapper>>();

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
            //获取conn
            ConnectionWrapper connectionWrapper = getConnectionWrapper(cluster,rabbitConfig);
            if(connectionWrapper == null){
                throw new RuntimeException("create rabbit conn failed.");
            }
            Connection connection = connectionWrapper.getConnection();
            //创建channel
            Channel channel = connection.createChannel(DEFAULT_CHANNEL_NUMBER);
            //计数+1
            connectionWrapper.incr();
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
     * 获取conn wrapper
     * @return
     */
    static ConnectionWrapper getConnectionWrapper(String cluster, RabbitConfig rabbitConfig){
        try {
            //若存己有conn且channel数未达到指定数量，则返回用来创建channel
            if(connectionWrapperMapping.get(cluster) != null){
                List<ConnectionWrapper> connectionWrapperList = connectionWrapperMapping.get(cluster);
                //TODO 按count排序取最小的
                ConnectionWrapper connectionWrapper = getAvalibleConnectionWrapper(connectionWrapperList);
                if(connectionWrapper.getCount() < MAX_CHANNEL_NUM){
                    return connectionWrapper;
                }
            }
            //否则，直接创建conn
            Connection conn = RabbitConnectionFactory.createConnection(cluster,rabbitConfig);
            return new ConnectionWrapper(conn);
        } catch (Exception e) {
            throw new RuntimeException("create rabbit conn failed:" + e);
        }
    }

    /**
     * 根据channel数获取可用的conn wrapper
     * @param connectionWrapperListList
     * @return
     */
    static ConnectionWrapper getAvalibleConnectionWrapper(List<ConnectionWrapper> connectionWrapperListList){
        return null;
    }

}
