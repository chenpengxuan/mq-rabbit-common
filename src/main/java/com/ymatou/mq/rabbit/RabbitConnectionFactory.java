package com.ymatou.mq.rabbit;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.AddressResolver;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.ymatou.mq.infrastructure.support.ScheduledExecutorHelper;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.RabbitConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * rabbit连接工厂
 * Created by zhangzhihua on 2017/3/27.
 */
public class RabbitConnectionFactory {

    private static final Logger logger = LoggerFactory.getLogger(RabbitConnectionFactory.class);

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
        AddressResolver addressResolver = new AddressResolver() {
            @Override
            public List<Address> getAddresses() throws IOException {
                List<Address> addressList = getRabbitAddresses(cluster,rabbitConfig);
                Collections.shuffle(addressList);

                //FIXME:直接logger.debug(addressList)
                if(!CollectionUtils.isEmpty(addressList)){
                    logger.debug("rabbit ip/port list...");
                    for(Address address:addressList){
                        logger.debug("ip:{},port:{}",address.getHost(),address.getPort());
                    }
                }
                return addressList;
            }
        };
        if(rabbitConfig.getWorkerThreadNum() > 0){
            Connection conn = connectionFactory.newConnection(Executors.newFixedThreadPool(rabbitConfig.getWorkerThreadNum()),addressResolver);
            return conn;
        }else{
            Connection conn = connectionFactory.newConnection(addressResolver);
            return  conn;
        }
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
            factory.setUsername(rabbitConfig.getUserName());
            factory.setPassword(rabbitConfig.getPassword());
            factory.setVirtualHost(rabbitConfig.getVirtualHost());
            factory.setAutomaticRecoveryEnabled(true);

            //FIXME:默认的不行吗
            factory.setHeartbeatExecutor(ScheduledExecutorHelper.newScheduledThreadPool(3, "rabbitmq-heartbeat-thread"));
            connFactoryMapping.put(cluster,factory);
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
            return toAddresses(rabbitConfig.getMasterAddress());
        }else{
            return toAddresses(rabbitConfig.getSlaveAddress());
        }
    }

    /**
     * FIXME:要更严谨些，必要的trim处理。" 10.10.10.1:4567 ; 10.10.10.2:4567"应该是合法的
     * 将url转换为address列表
     * @param url
     * @return
     */
    static List<Address> toAddresses(String url){
        List<Address> addressList = new ArrayList<Address>();
        String[] arr = url.split(";");
        for(String item:arr){
            //FIXME:直接调用Address.parseAddress()，可以支持单机域名
            String[] str = item.split(":");
            Address address = new Address(str[0],Integer.parseInt(str[1]));
            addressList.add(address);
        }
        return addressList;
    }


}
