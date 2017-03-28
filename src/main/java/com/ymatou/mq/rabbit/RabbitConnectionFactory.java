package com.ymatou.mq.rabbit;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.RabbitConstants;
import com.ymatou.mq.rabbit.support.ScheduledExecutorHelper;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * rabbit连接工厂
 * Created by zhangzhihua on 2017/3/27.
 */
public class RabbitConnectionFactory {

    /**
     * 连接工厂映射表
     */
    private static Map<String,ConnectionFactory> connFactoryMapping = new ConcurrentHashMap<String,ConnectionFactory>();

    /**
     * 创建连接
     * @return
     * @param cluster 集群名称 master/slave
     * @param rabbitConfig
     */
    public static Connection createConnection(String cluster, RabbitConfig rabbitConfig) throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        //获取连接工厂
        ConnectionFactory connectionFactory = getConnectionFactory(cluster, rabbitConfig);
        //创建连接
        return connectionFactory.newConnection();
    }

    /**
     * 根据集群名称创建连接工厂
     * @param cluster
     * @param rabbitConfig
     */
    static ConnectionFactory getConnectionFactory(String cluster, RabbitConfig rabbitConfig) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        if(connFactoryMapping.get(cluster) != null){
            return connFactoryMapping.get(cluster);
        }else{
            ConnectionFactory factory = new ConnectionFactory();
            String clusterUri = getClusterUrl(cluster, rabbitConfig);
            factory.setUri(clusterUri);
            factory.setAutomaticRecoveryEnabled(true);
            factory.setHeartbeatExecutor(ScheduledExecutorHelper.newScheduledThreadPool(3, "rabbitmq-heartbeat-thread|" + clusterUri));
            return factory;
        }
    }

    /**
     * 根据集群名称获取集群url
     * @param cluster
     * @param rabbitConfig
     * @return
     */
    static String getClusterUrl(String cluster, RabbitConfig rabbitConfig){
        if(RabbitConstants.CLUSTER_MASTER.equals(cluster)){
            return rabbitConfig.getMasterUri();
        }else{
            return rabbitConfig.getSlaveUri();
        }
    }
}
