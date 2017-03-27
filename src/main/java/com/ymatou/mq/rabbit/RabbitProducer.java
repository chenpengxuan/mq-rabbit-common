package com.ymatou.mq.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * rabbit生产者
 * Created by zhangzhihua on 2017/3/23.
 */
public class RabbitProducer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitProducer.class);

    /**
     * 交换器名称
     */
    private String exchange = null;

    /**
     * 路由KEY
     */
    private String routingKey = null;

    /**
     * 队列名称
     */
    private String queue = null;

    /**
     * 当前rabbit集群，默认master
     */
    private String cluster = CLUSTER_MASTER;

    /**
     * rabbit集群-master
     */
    private static final String CLUSTER_MASTER = "master";

    /**
     * rabbit集群-slave
     */
    private static final String CLUSTER_SLAVE = "slave";

    /**
     * 默认一个连接创建通道数目
     */
    private static final int CHANNEL_NUMBER = 10;

    /**
     * master通道
     */
    private Channel masterChannel;

    /**
     * slave通道
     */
    private Channel slaveChannel;

    /**
     * rabbit ack事件监听
     */
    private ConfirmListener confirmListener;

    /**
     * rabbit配置信息
     */
    private Properties props;

    public RabbitProducer(String appId, String bizCode, ConfirmListener confirmListener,Properties props){
        this.exchange = appId;
        this.routingKey = bizCode;
        //TODO 确认
        this.queue = bizCode;
        this.confirmListener = confirmListener;
        this.props = props;
        //初始化创建channel
        this.initChannel(props);
    }

    /**
     * 创建通道
     */
    void initChannel(Properties props){
        try {
            if(this.isMasterEnable(props)){
                this.masterChannel = this.createChannel(CLUSTER_MASTER);
            }
            if(this.isSlaveEnable(props)){
                this.slaveChannel = this.createChannel(CLUSTER_SLAVE);
            }
        } catch (Exception e) {
            throw new RuntimeException("create rabbit conn failed",e);
        }
    }

    /**
     * 创建生产通道
     * @return
     */
    Channel createChannel(String cluster){
        try {
            //创建conn
            Connection conn = this.createConnection(cluster);
            //创建channel
            Channel channel = conn.createChannel(CHANNEL_NUMBER);
            channel.exchangeDeclare(exchange, "direct", true);
            channel.queueDeclare(queue, true, false, false, null);
            channel.queueBind(queue, exchange, queue);
            channel.basicQos(1);
            if(channel != null){
                channel.addConfirmListener(this.confirmListener);
            }
            return channel;
        } catch (Exception e) {
            throw new RuntimeException("create rabbit channel failed.",e);
        }
    }

    /**
     * 创建conn
     * @return
     */
    Connection createConnection(String cluster){
        //创建conn
        Connection conn = null;
        try {
            conn = RabbitConnectionFactory.createConnection(cluster,props);
        } catch (Exception e) {
            throw new RuntimeException("create rabbit conn failed:" + e);
        }
        if(conn == null){
            throw new RuntimeException("create rabbit conn failed.");
        }
        return conn;
    }

    /**
     * 发布消息
     * @param
     * @param clientMsgId
     * @param msgId @throws IOException
     * @param props
     */
    public void publish(String body, String clientMsgId, String msgId, Properties props) throws IOException {
        try {
            //若master/slave都没有开启
            if(!this.isMasterEnable(props) && !this.isSlaveEnable(props)){
                throw new RuntimeException("master and slave not enable.");
            }

            if(this.isMasterEnable(props)){//若master开启

                AMQP.BasicProperties basicProps = new AMQP.BasicProperties.Builder()
                        .messageId(clientMsgId).correlationId(msgId)
                        .type(CLUSTER_MASTER)
                        .build();
                this.cluster = CLUSTER_MASTER;
                //import org.apache.commons.lang.SerializationUtils;
                //TODO convert string to bytes
                byte[] bd = null;
                this.getMasterChannel().basicPublish(exchange, routingKey, basicProps, bd);
            }else if(this.isSlaveEnable(props)){//若slave开启
                AMQP.BasicProperties basicProps = new AMQP.BasicProperties.Builder()
                        .messageId(clientMsgId).correlationId(msgId)
                        .type(CLUSTER_SLAVE)
                        .build();
                this.cluster = CLUSTER_SLAVE;
                this.getSlaveChannel().basicPublish(exchange, routingKey, basicProps, null);
            }
        } catch (IOException e) {
            logger.error("publish msg error with cluster:{},msgId:{},msgUuid:{}", cluster,clientMsgId, msgId,e);
        }
    }

    /**
     * 获取master channel
     * @return
     */
    public Channel getMasterChannel() {
        if(masterChannel == null){
            masterChannel = this.createChannel(CLUSTER_MASTER);
            if(masterChannel == null){
                throw new RuntimeException("create master channel fail.");
            }
        }
        return masterChannel;
    }

    /**
     * 获取slave channel
     * @return
     */
    public Channel getSlaveChannel() {
        if(slaveChannel == null){
            slaveChannel = this.createChannel(CLUSTER_SLAVE);
            if(slaveChannel == null){
                throw new RuntimeException("create slave channel fail.");
            }
        }
        return slaveChannel;
    }

    /**
     * 判断master是否开启
     * @param props
     * @return
     */
    boolean isMasterEnable(Properties props){
        //TODO
        return true;
    }

    /**
     * 判断slave是否开启
     * @param props
     * @return
     */
    boolean isSlaveEnable(Properties props){
        //TODO
        return true;
    }
}
