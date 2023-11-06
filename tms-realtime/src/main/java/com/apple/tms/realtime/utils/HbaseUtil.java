package com.apple.tms.realtime.utils;

import com.apple.tms.realtime.common.TmsConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseUtil {
    private static Connection conn;

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", TmsConfig.HBASE_ZOOKEEPER_QUORUM);
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //创建表
    public static void createTable(String nameSpace ,String tableMame,String ... families ) {

            Admin admin = null;
        try {
            if (families.length < 1) {
                System.out.println("至少需要一个列族");
                return;
            }
            admin =  conn.getAdmin();
            //判断是否存在
            if (admin.tableExists(TableName.valueOf(nameSpace, tableMame))) {
                System.out.println(nameSpace + ":" + tableMame + "已存在");
                return;
            }
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, tableMame));
            for (String family : families) {
                ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                builder.setColumnFamily(familyDescriptor);
            }

            admin.createTable(builder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    //向HBase 插入一条数据
    public static void putRow(String namespace ,String tableName,Put put) {
        BufferedMutator mutator = null;

        try {
            BufferedMutatorParams params
                    = new BufferedMutatorParams(TableName.valueOf(namespace,tableName));
            params.writeBufferSize(5 * 1024 * 1024);
            params.setWriteBufferPeriodicFlushTimeoutMs(3000L);
            mutator = conn.getBufferedMutator(params);
            mutator.mutate(put);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (mutator != null) {
                try {
                    mutator.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
