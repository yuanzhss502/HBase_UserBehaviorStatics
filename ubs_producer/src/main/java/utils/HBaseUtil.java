package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;

public class HBaseUtil {

    public static boolean isExistTable(Configuration conf, String tableName) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        boolean result = admin.tableExists(TableName.valueOf(tableName));

        admin.close();
        connection.close();

        return result;

    }

    public static void initNamespace(Configuration conf, String namespace) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        NamespaceDescriptor nd = NamespaceDescriptor
                .create(namespace)
                .addConfiguration("CreateTime", String.valueOf(System.currentTimeMillis()))
                .addConfiguration("AUTHOR", "Yuanzhss")
                .build();

        admin.createNamespace(nd);

    }


    /**
     * 这里未完成预分区
     * @param conf
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(Configuration conf, String tableName, int regions, String ... columnFamily) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

        for (String cf: columnFamily) {

            htd.addFamily(new HColumnDescriptor(cf));
        }

        System.out.println("before create");
        //建表时创建协处理器
        htd.addCoprocessor("hbase.CalleeWriteObserver");
        System.out.println("after create");

        admin.createTable(htd, genSplitKeys(regions));

        admin.close();
        connection.close();

    }

    private static byte[][] genSplitKeys(int regions) {


        //定义一个存放分区键的数组
        String[] keys = new String[regions];
        //目前推算region个数不会超过两位数所以region分区格式化为两位数所代表的的字符串
        DecimalFormat df = new DecimalFormat("00");

        for (int i = 0; i < regions; i ++) {

            keys[i] = df.format(i) + "|";

        }

        byte[][] splitKeys = new byte[regions][];
        //生成byte[][]类型的分区键时，一定要保证分区键时有序的
        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);

        for (int i = 0; i < regions; i ++) {

            treeSet.add(Bytes.toBytes(keys[i]));

        }

        Iterator<byte[]> splitKeysIterator = treeSet.iterator();
        int index= 0;
        while (splitKeysIterator.hasNext()) {

            byte[] b = splitKeysIterator.next();
            splitKeys[index ++] = b;

        }
        return splitKeys;
    }

    /**
     * 设计生成rowkey
     * regionCode_call1_buildTime_call2_flag_duration
     * @return
     */
    public static String genRowkey(String regionCode, String call1, String buildTime, String call2, String flag, String duration) {


        StringBuilder sb = new StringBuilder();
        sb.append(regionCode + "_")
                .append(call1 + "_")
                .append(buildTime + "_")
                .append(call2 +"_")
                .append(flag + "_")
                .append(duration);


        return sb.toString();
    }

    public static String genRegionCode (String call1, String buildTime, int regions) {
        int len = call1.length();
        //取出后四位数字
        String lastPhone = call1.substring(len-4);
        //取出年月
        String ym = buildTime.replaceAll("-", "")
                .replaceAll(":", "")
                .replaceAll(" ", "")
                .substring(0, 6);

        //离散操作1
        Integer x = Integer.valueOf(lastPhone) ^ Integer.valueOf(ym);
        //离散操作2
        Integer y = x.hashCode();
        //生成分区号
        int regionCode = y % regions;
        //格式化分区号
        DecimalFormat df = new DecimalFormat("00");

        return df.format(regionCode);


    }

}
