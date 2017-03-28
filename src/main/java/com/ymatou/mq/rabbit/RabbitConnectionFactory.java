package com.ymatou.mq.rabbit;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.AddressResolver;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.RabbitConstants;
import com.ymatou.mq.rabbit.support.ScheduledExecutorHelper;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
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
        Connection conn = connectionFactory.newConnection(new AddressResolver() {
            @Override
            public List<Address> getAddresses() throws IOException {
                List<Address> addressList = getRabbitAddresses(cluster,rabbitConfig);
                return addressList;
            }
        });
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
            factory.setVirtualHost(rabbitConfig.getVirtualHost());
            factory.setAutomaticRecoveryEnabled(true);
            //factory.setHeartbeatExecutor(ScheduledExecutorHelper.newScheduledThreadPool(3, "rabbitmq-heartbeat-thread|" + clusterUri));
            return factory;
        }
    }

    /**
     * 根据集群名称获取集群url
     * @param cluster
     * @param rabbitConfig
     * @return
     */
    static List<Address> getRabbitAddresses(String cluster, RabbitConfig rabbitConfig){
        if(RabbitConstants.CLUSTER_MASTER.equals(cluster)){
            return toAddresses(rabbitConfig.getMasterUri());
        }else{
            return toAddresses(rabbitConfig.getSlaveUri());
        }
    }

    /**
     * 将url转换为address列表
     * @param url
     * @return
     */
    static List<Address> toAddresses(String url){
        List<Address> addressList = new ArrayList<Address>();
        String[] arr = url.split(";");
        for(String item:arr){
            String[] str = item.split(":");
            Address address = new Address(str[0],Integer.parseInt(str[1]));
            addressList.add(address);
        }
        return addressList;
    }


}
