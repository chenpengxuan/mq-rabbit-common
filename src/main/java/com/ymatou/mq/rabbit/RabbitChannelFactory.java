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
 * FIXME:这个核心工厂类需要足够多的并发单元测试用例
 * rabbit channel创建工厂
 * Created by zhangzhihua on 2017/3/24.
 */
public class RabbitChannelFactory {

    private static final Logger logger = LoggerFactory.getLogger(RabbitChannelFactory.class);

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
     * channel wrapper列表
     */
    private static List<ChannelWrapper> channelWrapperList = Collections.synchronizedList(new ArrayList<ChannelWrapper>());

    /**
     * 获取channel wrapper
     * @param rabbitConfig
     * @return
     */
    public static ChannelWrapper getChannelWrapper(String cluster, RabbitConfig rabbitConfig) {
        if(RabbitConstants.CLUSTER_MASTER.equals(cluster)){
            return getChannelWrapper(cluster, rabbitConfig, masterChannelWrapperHolder);
        }else{
            return getChannelWrapper(cluster, rabbitConfig, slaveChannelWrapperHolder);
        }
    }

    /**
     * 获取channel wrapper
     *
     * @param cluster
     * @param rabbitConfig
     * @param channelWrapperHolder
     * @return
     */
    static ChannelWrapper getChannelWrapper(String cluster, RabbitConfig rabbitConfig, ThreadLocal<ChannelWrapper> channelWrapperHolder){
        ChannelWrapper channelWrapper = channelWrapperHolder.get();
        if(channelWrapper != null){
            return channelWrapper;
        }else{
            channelWrapper = RabbitChannelFactory.createChannelWrapper(cluster, rabbitConfig);
            channelWrapperHolder.set(channelWrapper);
            return channelWrapper;
        }
    }

    /**
     * 创建生产通道
     * @return
     */
    public static ChannelWrapper createChannelWrapper(String cluster, RabbitConfig rabbitConfig){
        try {
            //获取conn
            ConnectionWrapper connectionWrapper = getConnectionWrapper(cluster, rabbitConfig);

            Connection connection = connectionWrapper.getConnection();
            //创建channel
            Channel channel = connection.createChannel();

            channel.basicQos(1);
            logger.debug("createChannelWrapper,current thread name:{},thread id:{},channel:{}",Thread.currentThread().getName(),Thread.currentThread().getId(),channel.hashCode());
            //设置conn.channel数目+1
            connectionWrapper.incCount();

            ChannelWrapper channelWrapper = new ChannelWrapper(channel);
            channelWrapper.setConnectionWrapper(connectionWrapper);

            channelWrapper.setThread(Thread.currentThread());
            //添加recovery监听
            channelWrapper.addRecoveryListener();
            channelWrapperList.add(channelWrapper);

            return channelWrapper;
        } catch (Exception e) {
            throw new RuntimeException("create rabbit channel failed.",e);
        }
    }

    /**
     * 获取conn wrapper
     *
     * @param cluster
     * @param rabbitConfig
     * @return
     */
    static ConnectionWrapper getConnectionWrapper(String cluster, RabbitConfig rabbitConfig){
        if(RabbitConstants.CLUSTER_MASTER.equals(cluster)){
            return getConnectionWrapper(cluster, rabbitConfig, masterConnectionWrapperList);
        }else{
            return getConnectionWrapper(cluster, rabbitConfig, slaveConnectionWrapperList);
        }
    }

    /**
     * 获取ConnectionWrapper
     *
     * @param cluster
     * @param rabbitConfig
     * @param connectionWrapperList
     * @return
     */
    static ConnectionWrapper getConnectionWrapper(String cluster, RabbitConfig rabbitConfig, List<ConnectionWrapper> connectionWrapperList){
        try {
            //从现有连接中获取可用的conn wrapper
            ConnectionWrapper connectionWrapper = getConnectionWrapperOfHasAvailableChannels(connectionWrapperList,rabbitConfig);

            if(connectionWrapper != null){//若有可用
                return connectionWrapper;
            }else{//否则
                if(connectionWrapperList.size() < rabbitConfig.getMaxConnectionNum()){//若连接数未达上限，则直接创建
                    Connection connection = RabbitConnectionFactory.createConnection(cluster,rabbitConfig);
                    connectionWrapper = new ConnectionWrapper(connection);
                    connectionWrapperList.add(connectionWrapper);
                    return connectionWrapper;
                }else{//若连接数已达上限，则从现有连接中选择channel数目最小的
                    return getConnectionWrapperOfHasMinChannels(connectionWrapperList,rabbitConfig);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("create rabbit conn failed.",e);
        }
    }

    /**
     * 从现有连接中获取还有可用channel的conn wrapper
     * @param connectionWrapperList
     * @return
     */
    static ConnectionWrapper getConnectionWrapperOfHasAvailableChannels(List<ConnectionWrapper> connectionWrapperList,RabbitConfig rabbitConfig){
        if(CollectionUtils.isEmpty(connectionWrapperList)){
            return null;
        }

        //FIXME:不要每次选取时都排序一次，往connectionList添加时主动sort一次? 注意刚添加，还未sort的瞬间并发
        // 按channel数排序并取第一个
        ConnectionWrapper connectionWrapper = connectionWrapperList.stream().sorted(Comparator.comparing(ConnectionWrapper::getChannelCount))
                .findFirst().get();
        //若不超过指定数量，则返回
        if(connectionWrapper.getChannelCount() < rabbitConfig.getCoreChannelNum()){
            return  connectionWrapper;
        }
        return null;
    }

    /**
     * 从现有连接中获取持有最小channel数的conn wrapper
     * @param connectionWrapperList
     * @return
     */
    static ConnectionWrapper getConnectionWrapperOfHasMinChannels(List<ConnectionWrapper> connectionWrapperList,RabbitConfig rabbitConfig){
        //FIXME:不要每次选取时都排序一次，往connectionList添加时主动sort一次? 注意刚添加，还未sort的瞬间并发
        // 按channel数排序并取第一个
        ConnectionWrapper connectionWrapper = connectionWrapperList.stream().sorted(Comparator.comparing(ConnectionWrapper::getChannelCount))
                .findFirst().get();
        return connectionWrapper;
    }

    public static List<ChannelWrapper> getChannelWrapperList() {
        return channelWrapperList;
    }

}
