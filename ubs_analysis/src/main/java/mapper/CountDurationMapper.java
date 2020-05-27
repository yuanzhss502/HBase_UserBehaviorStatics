package mapper;

import kv.key.ComDimension;
import kv.key.ContactDimension;
import kv.key.DateDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CountDurationMapper extends TableMapper<ComDimension, Text> {
    private Map<String, String> phoneNameMap;
    private Text durationText = new Text();
    private ComDimension comDimension = new ComDimension();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        phoneNameMap = new HashMap<>();

        phoneNameMap.put("13600267890", "李敏");
        phoneNameMap.put("13653763443", "御神风");
        phoneNameMap.put("15463676544", "叶子铭");
        phoneNameMap.put("17875543455", "李自成");
        phoneNameMap.put("15454830989", "王百万");
        phoneNameMap.put("13543958909", "李米");
        phoneNameMap.put("18937482744", "王侯");
        phoneNameMap.put("17394784029", "沈明");
        phoneNameMap.put("13242098530", "陈留");
        phoneNameMap.put("14349038543", "乌苏");
        phoneNameMap.put("14324355779", "百威");
        phoneNameMap.put("15454738433", "李乐");
        phoneNameMap.put("19573897443", "王生");
        phoneNameMap.put("13324324390", "陈深");
        phoneNameMap.put("13435432294", "李生是");
        phoneNameMap.put("14325432333", "刘晨曦");
        phoneNameMap.put("13454324678", "吴明杰");
        phoneNameMap.put("19743857324", "李晨星");
        phoneNameMap.put("15787434345", "樊子源");
        phoneNameMap.put("16498538444", "刘生煎");

    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //05_15899098789_20200406122034_13988909888_1_1290
        String rowKey = Bytes.toString(key.get());
        String[] splitRowKey = rowKey.split("_");

        if (splitRowKey[4].equals("0")) return;

        //一下数据全部是主叫数据，但包含了被叫电话的数据

        String caller = splitRowKey[1];
        String buildTime = splitRowKey[2];
        String callee = splitRowKey[3];
        String duration = splitRowKey[5];
        durationText.set(duration);

        String year = buildTime.substring(0, 4);
        String month = buildTime.substring(4, 6);
        String day = buildTime.substring(6, 8);

        //组装ComDimension
        //组装DateDimension
        DateDimension yearDateDimension = new DateDimension(year, "-1", "-1");
        DateDimension monthDateDimension = new DateDimension(year, month, "-1");
        DateDimension dayDateDimension = new DateDimension(year, month, day);
        //组装ContactDimension
        ContactDimension callerContactDimension = new ContactDimension(caller, phoneNameMap.get(caller));

        //开始聚合主叫数据
        //年
        comDimension.setContactDimension(callerContactDimension);
        comDimension.setDateDimension(yearDateDimension);
        context.write(comDimension, durationText);
        //月
        comDimension.setContactDimension(callerContactDimension);
        comDimension.setDateDimension(monthDateDimension);
        context.write(comDimension, durationText);
        //日
        comDimension.setContactDimension(callerContactDimension);
        comDimension.setDateDimension(dayDateDimension);
        context.write(comDimension, durationText);
    }


}
