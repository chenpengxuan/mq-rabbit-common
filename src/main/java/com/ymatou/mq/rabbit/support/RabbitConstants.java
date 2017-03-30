package com.ymatou.mq.rabbit.support;

/**
 * rabbit常量
 * Created by zhangzhihua on 2017/3/28.
 */
public class RabbitConstants {

    //rabbit集群-master
    public static final String CLUSTER_MASTER = "master";
    //rabbit集群-slave
    public static final String CLUSTER_SLAVE = "slave";

    //DELIVERY模式-不持久化 non-persistent
    public static final int DELIVERY_NON_PERSISTENT = 1;
    //DELIVERY模式-持久化 persistent
    public static final int DELIVERY_PERSISTENT = 2;

    //属性字段定义
    public static final String MSG_ID = "msgId";
    public static final String BIZ_ID = "bizId";
    public static final String BODY = "body";
    public static final String QUEUE_CODE = "QUEUE_CODE";

}
