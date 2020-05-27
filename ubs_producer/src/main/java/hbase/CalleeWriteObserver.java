package hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;


public class CalleeWriteObserver extends BaseRegionObserver {

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);

        //获取你想要操作的目标表的名称
        String targetTableName = PropertiesUtil.getProperty("hbase.calllog.tablename");

        //获取当前成功put的了数据的表，可以与操作目标表不一致
        String currentTableName = e.getEnvironment().getRegionInfo().getTable().getNameAsString();

        if (!targetTableName.equals(currentTableName)) return;


        //01_17899087897_20200422133687_18299098907_1_0908
        String oriRowkey = Bytes.toString(put.getRow());

        String[] splitOriRowkey = oriRowkey.split("_");

        String oldFrag = splitOriRowkey[4];
        //如果当前插入的被叫数据，则直接返回，因为这里默认处理的是主叫数据
        if (oldFrag.equals("0")) return;


        int regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));

        String caller = splitOriRowkey[1];
        String buildTime = splitOriRowkey[2];
        String callee = splitOriRowkey[3];
        String duration = splitOriRowkey[5];

        String calleeRegionCode = HBaseUtil.genRegionCode(callee, buildTime, regions);
        String calleeRowkey = HBaseUtil.genRowkey(calleeRegionCode, callee, buildTime, caller, "0", duration);

        SimpleDateFormat sdf3 = new SimpleDateFormat("yyyyMMddHHmmss");
        //生成buildTimeTS
        String buildTimeTS = "";
        try {
            buildTimeTS = String.valueOf(sdf3.parse(buildTime));
        } catch (ParseException ex) {
            ex.printStackTrace();
        }

        Put calleePut = new Put(Bytes.toBytes(calleeRowkey));

        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(callee));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(caller));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTimeTS"), Bytes.toBytes(caller));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes("0"));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTimeTS"), Bytes.toBytes(buildTimeTS));


        Table table = e.getEnvironment().getTable(TableName.valueOf(targetTableName));
        table.put(calleePut);
        table.close();



    }
}
