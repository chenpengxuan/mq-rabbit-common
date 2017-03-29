package com.ymatou.mq.rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.ConnectionWrapper;
import com.ymatou.mq.rabbit.support.RabbitConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.Comparator;
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
     * connection wrapper映射表
     */
    private static Map<String,List<ConnectionWrapper>> connectionWrapperMapping = new ConcurrentHashMap<String,List<ConnectionWrapper>>();

    /**
     * master channel上下文
     */
    private static ThreadLocal<Channel> masterChannelHolder = new ThreadLocal<Channel>();

    /**
     * slave channel上下文
     */
    private static ThreadLocal<Channel> slaveChannelHolder = new ThreadLocal<Channel>();

    /**
     * 获取channel
     * @param rabbitConfig
     * @return
     */
    public static Channel getChannel(RabbitConfig rabbitConfig) {
        if(RabbitConstants.CLUSTER_MASTER.equals(rabbitConfig.getCurrentCluster())){
            return getChannel(rabbitConfig,masterChannelHolder);
        }else{
            return getChannel(rabbitConfig,slaveChannelHolder);
        }
    }

    /**
     * 获取channel
     * @param rabbitConfig
     * @param channelHolder
     * @return
     */
    static Channel getChannel(RabbitConfig rabbitConfig,ThreadLocal<Channel> channelHolder){
        Channel channel = channelHolder.get();
        if(channel != null){
            return channel;
        }else{
            channel = RabbitChannelFactory.createChannel(rabbitConfig);
            if(channel == null){
                throw new RuntimeException("create channel fail.");
            }
            channelHolder.set(channel);
            return channel;
        }
    }

    /**
     * 创建生产通道
     * @return
     */
    static Channel createChannel(RabbitConfig rabbitConfig){
        try {
            //获取conn
            ConnectionWrapper connectionWrapper = getConnectionWrapper(rabbitConfig);
            if(connectionWrapper == null){
                throw new RuntimeException("create rabbit conn failed.");
            }
            Connection connection = connectionWrapper.getConnection();
            //创建channel
            Channel channel = connection.createChannel(DEFAULT_CHANNEL_NUMBER);
            //设置conn.channel数目+1
            connectionWrapper.incCount();
            return channel;
        } catch (Exception e) {
            throw new RuntimeException("create rabbit channel failed.",e);
        }
    }

    /**
     * 获取conn wrapper
     * @param rabbitConfig
     * @return
     */
    static ConnectionWrapper getConnectionWrapper(RabbitConfig rabbitConfig){
        try {
            String cluster = rabbitConfig.getCurrentCluster();
            //若该集群存在己有conn，则查找channel数未达到最大数量的conn
            if(connectionWrapperMapping.get(cluster) != null){
                List<ConnectionWrapper> connectionWrapperList = connectionWrapperMapping.get(cluster);
                if(!CollectionUtils.isEmpty(connectionWrapperList)){
                    ConnectionWrapper connectionWrapper = getAvalibleConnectionWrapper(connectionWrapperList);
                    if(connectionWrapper != null && connectionWrapper.getCount() < MAX_CHANNEL_NUM){
                        return connectionWrapper;
                    }
                }
            }
            //否则，直接创建conn
            Connection conn = RabbitConnectionFactory.createConnection(cluster,rabbitConfig);
            ConnectionWrapper connectionWrapper = new ConnectionWrapper(conn);
            connectionWrapperMapping.get(cluster).add(connectionWrapper);
            return connectionWrapper;
        } catch (Exception e) {
            throw new RuntimeException("create rabbit conn failed:" + e);
        }
    }

    /**
     * 从现有连接中获取channel数未达到最大值的conn wrapper
     * @param connectionWrapperList
     * @return
     */
    static ConnectionWrapper getAvalibleConnectionWrapper(List<ConnectionWrapper> connectionWrapperList){
        // 获取连接池中Channel数量最小的连接
        ConnectionWrapper connectionWrapper = connectionWrapperList.stream().sorted(Comparator.comparing(ConnectionWrapper::getCount))
                .findFirst().get();
        return connectionWrapper;
    }

}
