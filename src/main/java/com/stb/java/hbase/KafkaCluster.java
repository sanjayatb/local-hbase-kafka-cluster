package com.stb.java.hbase;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaCluster {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCluster.class);
    public static final String KAFKA_SERVER_PROPERTIES_FILE = "kafka-server.properties";
    public static final String KAFKA_TOPIC_PROPERTIES_FILE = "kafka-topics.properties";
    public static final String KAFKA_CONSUMER_PROPERTIES_FILE = "kafka-consumer.properties";
    public static final String KAFKA_PRODUCER_PROPERTIES_FILE = "kafka-producer.properties";


    private static KafkaServerStartable serverStartable;


    public static void startKafka() throws IOException{
        Properties properties = loadProperties(KAFKA_SERVER_PROPERTIES_FILE);

        String host = properties.getProperty("zookeeper.connect");
        if(StringUtils.isBlank(host)){
            LOGGER.error("Fail to start server. No host name");
            return;
        }
        LOGGER.info("Local KafkaServer Started on [{}]",host);

        File file = new File(System.getProperty("java.io.tmpdir"));
        if(file.exists() && file.isDirectory()){
            FileUtils.deleteDirectory(file);
        }
        file.mkdir();

        properties.put("log.dir",file.getAbsolutePath());
        KafkaConfig kafkaConfig = new KafkaConfig(properties);

        serverStartable = new KafkaServerStartable(kafkaConfig);
        serverStartable.startup();

        createTopics(loadProperties(KAFKA_CONSUMER_PROPERTIES_FILE));
    }

    private static void createTopics(Properties properties) {
        ZkClient zkClient = null;

        try{
            String zookeeperHost = properties.getProperty("zookeeper.connect");
            int sessionTimeout = Integer.parseInt(properties.getProperty("topic.sessionTimeout"));
            int connectionTimeout = Integer.parseInt(properties.getProperty("topic.connectionTimeout"));
            int partitions = Integer.parseInt(properties.getProperty("topic.partitions"));
            int replication = Integer.parseInt(properties.getProperty("topic.replication"));

            zkClient = new ZkClient(zookeeperHost,sessionTimeout,connectionTimeout, ZKStringSerializer$.MODULE$);
            ZkUtils zkUtils = new ZkUtils(zkClient,new ZkConnection(zookeeperHost),false);

            listTopics().forEach( topicName -> {
                AdminUtils.createTopic(zkUtils,topicName,partitions,
                        replication,new Properties(), RackAwareMode.Safe$.MODULE$);

                LOGGER.info("{} topic created",topicName);
            } );

        }catch (Exception e){
            LOGGER.error("Fail to create the topics",e);
        }finally {
            if(zkClient != null){
                zkClient.close();
            }
        }
    }

    private static List<String> listTopics() {
        Properties properties = loadProperties(KAFKA_TOPIC_PROPERTIES_FILE);
        List<String> topics = new ArrayList<>();
        properties.forEach((key,value) -> {
            String keyStr = (String) key;
            if(keyStr.contains(".topic") && value instanceof String){
                topics.add((String) value);
            }
        });
        return topics;
    }

    private static Properties loadProperties(String file) {
        try(InputStream is = HBaseCluster.class.getClassLoader().getResourceAsStream(file)){
            Properties properties = new Properties();
            properties.load(is);
            return properties;
        }catch (Exception e){
            LOGGER.error("Fail to load properties",e);
        }
        return null;
    }


}
