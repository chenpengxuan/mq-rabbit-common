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
     * 最大允许创建连接数
     */
    private static final int MAX_CONNECTION_NUM = 100;

    /**
     * master conn wrapper列表
     */
    private static List<ConnectionWrapper> masterConnectionWrapperList = Collections.synchronizedList(new ArrayList<ConnectionWrapper>());

    /**
     * slave conn wrapper列表
     */
    private static List<ConnectionWrapper> slaveConnectionWrapperList = Collections.synchronizedList(new ArrayList<ConnectionWrapper>());

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
            channelWrapper.setConnectionWrapper(connectionWrapper);
            //添加recovery监听
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
        if(RabbitConstants.CLUSTER_MASTER.equals(rabbitConfig.getCurrentCluster())){
            return getConnectionWrapper(rabbitConfig,masterConnectionWrapperList);
        }else{
            return getConnectionWrapper(rabbitConfig,slaveConnectionWrapperList);
        }
    }

    /**
     * 获取ConnectionWrapper
     * @param rabbitConfig
     * @param connectionWrapperList
     * @return
     */
    static ConnectionWrapper getConnectionWrapper(RabbitConfig rabbitConfig,List<ConnectionWrapper> connectionWrapperList){
        try {
            //从现有连接中获取可用的conn wrapper
            ConnectionWrapper connectionWrapper = getConnectionWrapperOfHasAvailableChannels(connectionWrapperList);

            if(connectionWrapper != null){//若有可用
                return connectionWrapper;
            }else{//否则
                if(connectionWrapperList.size() < MAX_CONNECTION_NUM){//若连接数未达上限，则直接创建
                    Connection connection = RabbitConnectionFactory.createConnection(rabbitConfig.getCurrentCluster(),rabbitConfig);
                    connectionWrapper = new ConnectionWrapper(connection);
                    connectionWrapperList.add(connectionWrapper);
                    return connectionWrapper;
                }else{//若连接数已达上限，则从现有连接中选择channel数目最小的
                    return getConnectionWrapperOfHasMinChannels(connectionWrapperList);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("create rabbit conn failed:" + e);
        }
    }

    /**
     * 从现有连接中获取还有可用channel的conn wrapper
     * @param connectionWrapperList
     * @return
     */
    static ConnectionWrapper getConnectionWrapperOfHasAvailableChannels(List<ConnectionWrapper> connectionWrapperList){
        if(CollectionUtils.isEmpty(connectionWrapperList)){
            return null;
        }
        // 按channel数排序并取第一个
        ConnectionWrapper connectionWrapper = connectionWrapperList.stream().sorted(Comparator.comparing(ConnectionWrapper::getCount))
                .findFirst().get();
        //若不超过指定数量，则返回
        if(connectionWrapper.getCount() < CORE_CHANNEL_NUM){
            return  connectionWrapper;
        }
        return null;
    }

    /**
     * 从现有连接中获取持有最小channel数的conn wrapper
     * @param connectionWrapperList
     * @return
     */
    static ConnectionWrapper getConnectionWrapperOfHasMinChannels(List<ConnectionWrapper> connectionWrapperList){
        // 按channel数排序并取第一个
        ConnectionWrapper connectionWrapper = connectionWrapperList.stream().sorted(Comparator.comparing(ConnectionWrapper::getCount))
                .findFirst().get();
        return connectionWrapper;
    }

}
