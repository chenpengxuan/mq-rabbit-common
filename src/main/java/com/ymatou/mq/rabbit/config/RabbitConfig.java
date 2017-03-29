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
     * FIXME:以下两项移至receiver biz.properties，只在receiver才有效
     */
    /**
     * 是否开启master集群，默认开启
     */
    private boolean masterEnable = true;

    /**
     * 是否开启slave集群，默认开启
     */
    private boolean slaveEnable = true;

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

    @DisconfFileItem(name = "rabbitmq.master.enable")
    public boolean isMasterEnable() {
        return masterEnable;
    }

    public void setMasterEnable(boolean masterEnable) {
        this.masterEnable = masterEnable;
    }

    @DisconfFileItem(name = "rabbitmq.slave.enable")
    public boolean isSlaveEnable() {
        return slaveEnable;
    }

    public void setSlaveEnable(boolean slaveEnable) {
        this.slaveEnable = slaveEnable;
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

    /**
     * 获取当前集群
     * @return
     */
    public String getCurrentCluster(){
        if(masterEnable){
            return RabbitConstants.CLUSTER_MASTER;
        }else if(slaveEnable){
            return RabbitConstants.CLUSTER_SLAVE;
        }else{
            return null;
        }
    }
}
