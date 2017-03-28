package com.ymatou.mq.rabbit;

import com.rabbitmq.client.ConfirmListener;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * rabbit生产者创建工厂
 * Created by zhangzhihua on 2017/3/24.
 */
public class RabbitProducerFactory {

    private static final Logger logger = LoggerFactory.getLogger(RabbitProducerFactory.class);

    /**
     * producer映射表
     */
    private static Map<String,RabbitProducer> producerMapping = new ConcurrentHashMap<String,RabbitProducer>();

    /**
     * create rabbit生产者
     * @param appId
     * @param queueCode
     * @param rabbitConfig
     * @return
     */
    public static RabbitProducer createRabbitProducer(String appId, String queueCode, ConfirmListener confirmListener, RabbitConfig rabbitConfig){
        String key = String.format("%s_%s",appId, queueCode);
        if(producerMapping.get(key) != null){
            return producerMapping.get(key);
        }else{
            RabbitProducer producer = new RabbitProducer(appId, queueCode,confirmListener, rabbitConfig);
            producerMapping.put(key,producer);
            return producer;
        }
    }
}
