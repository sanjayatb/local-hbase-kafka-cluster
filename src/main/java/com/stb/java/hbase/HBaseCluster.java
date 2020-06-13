package com.stb.java.hbase;

import org.apache.hadoop.hbase.HBaseTestingUtility;

public class HBaseCluster {

    private static HBaseTestingUtility miniCluster = new HBaseTestingUtility();

    public static void main(String[] args) throws Exception {
        miniCluster.getConfiguration().set("test.hbase.zookeeper.clientPort","2181");
        miniCluster.getConfiguration().set("test.build.data.basedirectory","src/main/resources/data");

        miniCluster.startMiniCluster();
    }


}
