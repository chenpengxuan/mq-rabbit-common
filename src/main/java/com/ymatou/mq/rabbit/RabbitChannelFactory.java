package com.ymatou.mq.rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.ChannelWrapper;
import com.ymatou.mq.rabbit.support.ConnectionWrapper;
import com.ymatou.mq.rabbit.support.RabbitConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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
    private static List<ConnectionWrapper> masterConnectionWrapperList = new CopyOnWriteArrayList<ConnectionWrapper>();

    /**
     * slave conn wrapper列表
     */
    private static List<ConnectionWrapper> slaveConnectionWrapperList = new CopyOnWriteArrayList<ConnectionWrapper>();

    /**
     * master channel wrapper上下文
     */
    private static ThreadLocal<ChannelWrapper> masterChannelWrapperHolder = new ThreadLocal<ChannelWrapper>();

    /**
     * slave channel wrapper上下文
     */
    private static ThreadLocal<ChannelWrapper> slaveChannelWrapperHolder = new ThreadLocal<ChannelWrapper>();

    /**
     * master channel wrapper列表
     */
    private static List<ChannelWrapper> masterChannelWrapperList = new CopyOnWriteArrayList<ChannelWrapper>();

    /**
     * slave channel wrapper列表
     */
    private static List<ChannelWrapper> slaveChannelWrapperList = new CopyOnWriteArrayList<ChannelWrapper>();

    /**
     * 获取channel wrapper
     * @param rabbitConfig
     * @return
     */
    public static ChannelWrapper getChannelWrapperByThreadContext(String cluster, RabbitConfig rabbitConfig) {
        if(RabbitConstants.CLUSTER_MASTER.equals(cluster)){
            return getChannelWrapperByThreadContext(cluster, rabbitConfig, masterChannelWrapperHolder);
        }else{
            return getChannelWrapperByThreadContext(cluster, rabbitConfig, slaveChannelWrapperHolder);
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
    static ChannelWrapper getChannelWrapperByThreadContext(String cluster, RabbitConfig rabbitConfig, ThreadLocal<ChannelWrapper> channelWrapperHolder){
        ChannelWrapper channelWrapper = channelWrapperHolder.get();
        if(channelWrapper != null && channelWrapper.getChannel() != null){
            //若channel状态正常
            if(channelWrapper.getChannel().isOpen()){
                return channelWrapper;
            }else{
                //若channel状态异常
                logger.warn("channel status exception,cluster:{},channel:{}.",cluster,channelWrapper.getChannel());
                releaseChannelWrapper(cluster,channelWrapper,true);
                //创建新的channel
                channelWrapper = RabbitChannelFactory.createChannelWrapper(cluster, rabbitConfig);
                channelWrapperHolder.set(channelWrapper);
                return channelWrapper;
            }

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
            channel.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException cause) {
                    logger.warn("One rabbitmq channel shutdownCompleted ", cause);
                }
            });

            logger.debug("createChannelWrapper,current thread name:{},thread id:{},channel:{}",Thread.currentThread().getName(),Thread.currentThread().getId(),channel.hashCode());
            //设置conn.channel数目+1
            connectionWrapper.incCount();

            ChannelWrapper channelWrapper = new ChannelWrapper(channel);
            channelWrapper.setConnectionWrapper(connectionWrapper);

            channelWrapper.setThread(Thread.currentThread());
            //添加recovery监听
            channelWrapper.addRecoveryListener();
            //添加到channelWrapper列表
            if(RabbitConstants.CLUSTER_MASTER.equals(cluster)){
                masterChannelWrapperList.add(channelWrapper);
            }else{
                slaveChannelWrapperList.add(channelWrapper);
            }

            //当channel变化如创建时，排序connectionWrapperList
            if(RabbitConstants.CLUSTER_MASTER.equals(cluster)){
                sortConnectionWrapperList(masterConnectionWrapperList);
            }else{
                sortConnectionWrapperList(slaveConnectionWrapperList);
            }
            return channelWrapper;
        } catch (Exception e) {
            throw new RuntimeException("create rabbit channel failed.",e);
        }
    }

    /**
     * 释放channel wrapper
     * @param cluster
     * @param isThreadContext
     */
    public static void releaseChannelWrapper(String cluster, ChannelWrapper channelWrapper, boolean isThreadContext){
        //关闭channel
        Channel channel = channelWrapper.getChannel();
        if(channel != null && channel.isOpen()){
            try {
                channel.close();
            } catch (Exception e) {
                logger.error("close channel error.",e);
            }
        }

        //清除channelWrapperHolder
        if(isThreadContext){
            if(channel != null){
                if(RabbitConstants.CLUSTER_MASTER.equals(cluster)){
                    masterChannelWrapperHolder.remove();
                }else{
                    slaveChannelWrapperHolder.remove();
                }
            }
        }

        //减conn.channel计数
        channelWrapper.getConnectionWrapper().decCount();

        //从channelWrapper列表移除
        if(RabbitConstants.CLUSTER_MASTER.equals(cluster)){
            masterChannelWrapperList.remove(channelWrapper);
        }else{
            slaveChannelWrapperList.remove(channelWrapper);
        }

        //当channel变化如创建时，排序connectionWrapperList
        if(RabbitConstants.CLUSTER_MASTER.equals(cluster)){
            sortConnectionWrapperList(masterConnectionWrapperList);
        }else{
            sortConnectionWrapperList(slaveConnectionWrapperList);
        }

        //清除channelWrapper
        channelWrapper = null;
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

            if(connectionWrapper != null && connectionWrapper.getConnection() != null && connectionWrapper.getConnection().isOpen()){//若有可用
                return connectionWrapper;
            }else{//若没有可用
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

        // 按channel数倒排序取第一个
        ConnectionWrapper connectionWrapper = connectionWrapperList.get(0);
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
        // 按channel数排序并取第一个
        ConnectionWrapper connectionWrapper = connectionWrapperList.get(0);
        return connectionWrapper;
    }

    /**
     * 对connWrapperList按持有channel数量倒序排序
     * @param connectionWrapperList
     */
    static void sortConnectionWrapperList(List<ConnectionWrapper> connectionWrapperList){
        //connectionWrapperList.stream().sorted(Comparator.comparing(ConnectionWrapper::getChannelCount));
        Collections.sort(connectionWrapperList);
        logger.debug("all connection num:{}.",connectionWrapperList.size());
        if(logger.isDebugEnabled()){
            for(ConnectionWrapper connectionWrapper:connectionWrapperList){
                logger.debug("current conn channels num:{}.",connectionWrapper.getChannelCount());
            }
        }
    }

    public static List<ChannelWrapper> getMasterChannelWrapperList() {
        return masterChannelWrapperList;
    }

    public static List<ChannelWrapper> getSlaveChannelWrapperList() {
        return slaveChannelWrapperList;
    }

    public static List<ChannelWrapper> getChannelWrapperList(String cluster) {
        if(RabbitConstants.CLUSTER_MASTER.equals(cluster)){
            return getMasterChannelWrapperList();
        }else{
            return getSlaveChannelWrapperList();
        }
    }

}
