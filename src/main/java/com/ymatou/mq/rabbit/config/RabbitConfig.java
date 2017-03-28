/*
 *
 * (C) Copyright 2017 Ymatou (http://www.ymatou.com/). All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.config;

import com.baidu.disconf.client.common.annotations.DisconfFile;
import com.baidu.disconf.client.common.annotations.DisconfFileItem;
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
    private String masterUri;

    /**
     * 备份集群uri
     */
    private String slaveUri;

    /**
     * 是否开启master集群
     */
    private int masterEnable;

    /**
     * 是否开启slave集群
     */
    private int slaveEnable;

    @DisconfFileItem(name = "rabbitmq.primary.uri")
    public String getMasterUri() {
        return masterUri;
    }

    public void setMasterUri(String masterUri) {
        this.masterUri = masterUri;
    }

    @DisconfFileItem(name = "rabbitmq.secondary.uri")
    public String getSlaveUri() {
        return slaveUri;
    }

    public void setSlaveUri(String slaveUri) {
        this.slaveUri = slaveUri;
    }

    public int getMasterEnable() {
        return masterEnable;
    }

    public void setMasterEnable(int masterEnable) {
        this.masterEnable = masterEnable;
    }

    public int getSlaveEnable() {
        return slaveEnable;
    }

    public void setSlaveEnable(int slaveEnable) {
        this.slaveEnable = slaveEnable;
    }
}
