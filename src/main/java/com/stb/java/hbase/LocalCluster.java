package com.stb.java.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalCluster {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalCluster.class);

    public static void main(String[] args) {
        try {
            HBaseCluster.startHbase();
            KafkaCluster.startKafka();
        } catch (Exception e) {
            LOGGER.error("Fail to start Local Cluster");
        }

    }

}
