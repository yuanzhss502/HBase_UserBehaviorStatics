package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class HBaseDAO {
    private int regions;
    private String nameSpace;
    private String tableName;
    public static final Configuration conf;
    private Table table;
    private Connection connection;
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");


    static {

        conf = HBaseConfiguration.create();
    }

    /**
     * hbase.calllog.regions = 6
     * hbase.calllog.namespace = ns_ct
     * hbase.calllog.tablename = ns_Ct:calllog
     */
    public HBaseDAO() {
        try {

            regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));
            nameSpace = PropertiesUtil.getProperty("hbase.calllog.namespace");
            tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
            connection = ConnectionFactory.createConnection(conf);

            if (!HBaseUtil.isExistTable(conf, tableName)) {

//                HBaseUtil.initNamespace(conf, nameSpace);
                HBaseUtil.createTable(conf, tableName, regions, "f1", "f2");

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * ori数据样式：17499768086, 18940098780, 2017-08-14 13:38:31, 1290
     * rowkey样式：01_17499768086_20170814133831_1_1290
     * @param ori
     */
    public void put(String ori) throws IOException {

        try {
            table = connection.getTable(TableName.valueOf(tableName));

            //18799098989, 13299090909, 2020-04-29, 0398
            String[] splitOri = ori.split(",");
            String caller = splitOri[0];
            String callee = splitOri[1];
            String buildTime = splitOri[2];
            String duration = splitOri[3];
            String regionCode = HBaseUtil.genRegionCode(caller, buildTime, regions);
            String buildTimeReplace = sdf2.format(sdf1.parse(buildTime));
            String buildTimeTS = String.valueOf(sdf1.parse(buildTime).getTime());

            //生成rowkey
            String rowkey = HBaseUtil.genRowkey(regionCode, caller, buildTimeReplace, callee, "1", duration);

            //向表中插入该条数据
            Put put = new Put(Bytes.toBytes(rowkey));

            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call1"), Bytes.toBytes(caller));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call2"), Bytes.toBytes(callee));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("buildTimeTS"), Bytes.toBytes(buildTimeTS));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

            table.put(put);


        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

    }

}
