/*
 *
 * (C) Copyright 2017 Ymatou (http://www.ymatou.com/). All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.config;

import com.baidu.disconf.client.common.annotations.DisconfFile;
import com.baidu.disconf.client.common.annotations.DisconfFileItem;
import com.ymatou.mq.rabbit.support.RabbitConstants;
import org.springframework.stereotype.Component;

/**
 * @author luoshiqian 2017/3/27 16:40
 */
@Component
@DisconfFile(fileName = "rabbitmq.properties")
public class RabbitConfig {

    /**
     * 主集群uri
     */
    private String masterAddress;

    /**
     * 备份集群uri
     */
    private String slaveAddress;

    /**
     * 用户名
     */
    private String userName;

    /**
     * 密码
     */
    private String password;

    /**
     * 虚拟目录
     */
    private String virtualHost;

    /**
     * 自定义conn worker线程数量
     */
    private int workerThreadNum;

    /**
     * 允许最大连接数目
     */
    private int maxConnectionNum;

    /**
     * 一个conn正常允许创建的channel数目，若conn超过最大数则conn.channel数可超过这个数目
     */
    private int coreChannelNum;

    private String basicQos;

    @DisconfFileItem(name = "rabbitmq.primary.address")
    public String getMasterAddress() {
        return masterAddress;
    }

    public void setMasterAddress(String masterAddress) {
        this.masterAddress = masterAddress;
    }

    @DisconfFileItem(name = "rabbitmq.secondary.address")
    public String getSlaveAddress() {
        return slaveAddress;
    }

    public void setSlaveAddress(String slaveAddress) {
        this.slaveAddress = slaveAddress;
    }

    @DisconfFileItem(name = "rabbitmq.virtual.host")
    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    @DisconfFileItem(name = "rabbitmq.password")
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @DisconfFileItem(name = "rabbitmq.username")
    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @DisconfFileItem(name = "worker.thread.num")
    public int getWorkerThreadNum() {
        return workerThreadNum;
    }

    public void setWorkerThreadNum(int workerThreadNum) {
        this.workerThreadNum = workerThreadNum;
    }

    @DisconfFileItem(name = "max.connection.num")
    public int getMaxConnectionNum() {
        return maxConnectionNum;
    }

    public void setMaxConnectionNum(int maxConnectionNum) {
        this.maxConnectionNum = maxConnectionNum;
    }

    @DisconfFileItem(name = "core.channel.num")
    public int getCoreChannelNum() {
        return coreChannelNum;
    }

    public void setCoreChannelNum(int coreChannelNum) {
        this.coreChannelNum = coreChannelNum;
    }

    public String getBasicQos() {
        return basicQos;
    }

    public void setBasicQos(String basicQos) {
        this.basicQos = basicQos;
    }
}
