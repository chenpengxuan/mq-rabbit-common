package com.ymatou.mq.rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.ChannelWrapper;
import com.ymatou.mq.rabbit.support.ConnectionWrapper;
import com.ymatou.mq.rabbit.support.RabbitConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * rabbit channel创建工厂
 * Created by zhangzhihua on 2017/3/24.
 */
public class RabbitChannelFactory {

    private static final Logger logger = LoggerFactory.getLogger(RabbitChannelFactory.class);

    /**
     * 一个conn正常允许创建的channel数目，若conn超过最大数则conn.channel数可超过这个数目
     */
    private static final int CORE_CHANNEL_NUM = 50;

    /**
     * connection wrapper映射表
     */
    private static Map<String,List<ConnectionWrapper>> connectionWrapperMapping = new ConcurrentHashMap<String,List<ConnectionWrapper>>();

    /**
     * master channel wrapper上下文
     */
    private static ThreadLocal<ChannelWrapper> masterChannelWrapperHolder = new ThreadLocal<ChannelWrapper>();

    /**
     * slave channel wrapper上下文
     */
    private static ThreadLocal<ChannelWrapper> slaveChannelWrapperHolder = new ThreadLocal<ChannelWrapper>();

    /**
     * 获取channel wrapper
     * @param rabbitConfig
     * @return
     */
    public static ChannelWrapper getChannelWrapper(RabbitConfig rabbitConfig) {
        if(RabbitConstants.CLUSTER_MASTER.equals(rabbitConfig.getCurrentCluster())){
            ChannelWrapper channelWrapper = getChannelWrapper(rabbitConfig, masterChannelWrapperHolder);
            logger.debug("getChannelWrapper,current thread name:{},thread id:{},channel:{}",Thread.currentThread().getName(),Thread.currentThread().getId(),channelWrapper.getChannel());
            return channelWrapper;
        }else{
            return getChannelWrapper(rabbitConfig,slaveChannelWrapperHolder);
        }
    }

    /**
     * 获取channel wrapper
     * @param rabbitConfig
     * @param channelWrapperHolder
     * @return
     */
    static ChannelWrapper getChannelWrapper(RabbitConfig rabbitConfig, ThreadLocal<ChannelWrapper> channelWrapperHolder){
        ChannelWrapper channelWrapper = channelWrapperHolder.get();
        if(channelWrapper != null){
            return channelWrapper;
        }else{
            channelWrapper = RabbitChannelFactory.createChannelWrapper(rabbitConfig);
            if(channelWrapper == null){
                throw new RuntimeException("create channel fail.");
            }
            channelWrapperHolder.set(channelWrapper);
            return channelWrapper;
        }
    }

    /**
     * 创建生产通道
     * @return
     */
    static ChannelWrapper createChannelWrapper(RabbitConfig rabbitConfig){
        try {
            //获取conn
            ConnectionWrapper connectionWrapper = getConnectionWrapper(rabbitConfig);
            if(connectionWrapper == null){
                throw new RuntimeException("create rabbit conn failed.");
            }
            Connection connection = connectionWrapper.getConnection();
            //创建channel
            Channel channel = connection.createChannel();
            logger.debug("createChannelWrapper,current thread name:{},thread id:{},channel:{}",Thread.currentThread().getName(),Thread.currentThread().getId(),channel.hashCode());
            //设置conn.channel数目+1
            connectionWrapper.incCount();

            ChannelWrapper channelWrapper = new ChannelWrapper(channel);
            channelWrapper.addRecoveryListener();

            return channelWrapper;
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
            if(!CollectionUtils.isEmpty(connectionWrapperMapping.get(cluster))){
                List<ConnectionWrapper> connectionWrapperList = connectionWrapperMapping.get(cluster);
                ConnectionWrapper connectionWrapper = getAvailableConnectionWrapper(connectionWrapperList);
                if(connectionWrapper != null && connectionWrapper.getCount() < CORE_CHANNEL_NUM){
                    return connectionWrapper;
                }
            }
            //否则，直接创建conn
            connectionWrapperMapping.put(cluster,new ArrayList<ConnectionWrapper>());
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
    static ConnectionWrapper getAvailableConnectionWrapper(List<ConnectionWrapper> connectionWrapperList){
        // 获取连接池中Channel数量最小的连接
        ConnectionWrapper connectionWrapper = connectionWrapperList.stream().sorted(Comparator.comparing(ConnectionWrapper::getCount))
                .findFirst().get();
        return connectionWrapper;
    }

}
