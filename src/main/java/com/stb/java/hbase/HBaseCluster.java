package com.stb.java.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class HBaseCluster {

    public static final Logger LOGGER = LoggerFactory.getLogger(HBaseCluster.class);
    private static HBaseTestingUtility miniCluster = new HBaseTestingUtility();
    public static final String HBASE_PROPERTY_FILE = "hbase.properties";
    public static final String TABLE_PREFIX = "hbase.table.name";
    private static final Properties PROPERTIES = new Properties();

    private HBaseCluster(){}

    public static void startHbase() throws Exception {
        loadProperties();
        
        miniCluster.getConfiguration().set("test.hbase.zookeeper.clientPort", (String) PROPERTIES.get("hbase.zookeeper.property.clientPort"));
        miniCluster.getConfiguration().setInt(HConstants.MASTER_INFO_PORT, -1);
        miniCluster.getConfiguration().setInt(HConstants.REGIONSERVER_PORT, -1);
        miniCluster.getConfiguration().set("test.build.data.basedirectory",System.getProperty("java.io.tmpdir"));

        miniCluster.startMiniCluster();
        
        createTables();
        
    }

    private static void createTables() throws IOException {
        Map<String, List<String>> tables = tables();
        LOGGER.info("Tables : {}",tables);
        Admin admin = miniCluster.getConnection().getAdmin();

        String nameSpace = PROPERTIES.getProperty("hbase.namespace");
        NamespaceDescriptor namespace = NamespaceDescriptor.create(nameSpace).build();
        admin.createNamespace(namespace);
        tables.forEach( (tableName,columns) -> {
            try {
                createNewTable(admin,nameSpace,tableName,columns);
            } catch (IOException e) {
                LOGGER.error("Fail to create table",e);
            }
        });
    }

    private static void createNewTable(Admin admin, String nameSpace, String tableName, List<String> columnFamilies) throws IOException {
        TableName table = TableName.valueOf(nameSpace,tableName);
        if(admin.tableExists(table)){
            if(admin.isTableEnabled(table)){
                admin.disableTable(table);
            }
            admin.truncateTable(table,true);
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(table);
            columnFamilies.forEach( colFamily -> {
                byte[] columnFamilyByteArr = colFamily.getBytes();
                descriptor.addFamily(new HColumnDescriptor(columnFamilyByteArr));
            });
            descriptor.addCoprocessor("org.apache.hadoop.coprocessor.AggregateImplementation");
            admin.createTable(descriptor);
            LOGGER.info("Table created : {}",tableName);
        }
    }

    private static Map<String, List<String>> tables() {
        Map<String, List<String>> tables = new HashMap<>();
        PROPERTIES.forEach( (key,value) -> {
            String keyStr = (String) key;
            if(keyStr.startsWith(TABLE_PREFIX)){
                String tableName = (String) value;
                String columStr = PROPERTIES.getProperty(TABLE_PREFIX+tableName.replace('-','.')+".columns");
                if(StringUtils.isBlank(columStr)){
                    LOGGER.error("Column not defined for {}",tableName);
                    return;
                }
                List<String> columns = Arrays.asList(columStr.split(","));
                tables.put(tableName,columns);
            }
        });
        return tables;
    }

    public static void clearTable(String tableName){
        loadProperties();
        try {
            Admin admin = miniCluster.getConnection().getAdmin();
            TableName table = TableName.valueOf(PROPERTIES.getProperty("hbase.namespace"),tableName);
            if(admin.tableExists(table)){
                admin.disableTable(table);
                admin.truncateTable(table,false);
            }
        } catch (IOException e) {
            LOGGER.error("Fail to create table. [{}]",tableName,e);
        }
    }


    private static void loadProperties() {
        try(InputStream is = HBaseCluster.class.getClassLoader().getResourceAsStream(HBASE_PROPERTY_FILE)){
            PROPERTIES.load(is);
        }catch (Exception e){
            LOGGER.error("Fail to load properties",e);
        }
    }
}
