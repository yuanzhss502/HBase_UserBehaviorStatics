package producer;


import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ProductLog {

    private String startTime = "2020-04-01";
    private String endTime = "2020-12-01";

    //生产数据

    //用于存放随机的手机号码
    private List<String> phoneList = new ArrayList<>();
    private Map<String, String> phoneNameMap = new HashMap<>();

    public void initPhone() {

        phoneList.add("13600267890");
        phoneList.add("13653763443");
        phoneList.add("15463676544");
        phoneList.add("17875543455");
        phoneList.add("15454830989");
        phoneList.add("13543958909");
        phoneList.add("18937482744");
        phoneList.add("17394784029");
        phoneList.add("13242098530");
        phoneList.add("14349038543");
        phoneList.add("14324355779");
        phoneList.add("15454738433");
        phoneList.add("19573897443");
        phoneList.add("13324324390");
        phoneList.add("13435432294");
        phoneList.add("14325432333");
        phoneList.add("13454324678");
        phoneList.add("19743857324");
        phoneList.add("15787434345");
        phoneList.add("16498538444");
        phoneList.add("17845748934");

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
        phoneNameMap.put("17845748934", "霍明浩");



    }

    /**
     * 形式: 13600267890, 13653763443, 2020-02-02 08:09:10, 0300
     * @param
     */

    public String product() {
        //初始化caller
        String caller = null;
        String callee = null;

        //初始化name
        String callerName = null;
        String calleeName = null;

        //随机选取号码
        int callerIndex = (int) (Math.random() * phoneList.size());
        caller = phoneList.get(callerIndex);
        callerName = phoneNameMap.get(caller);

        while (true) {

            int calleeIndex = (int) (Math.random() * phoneList.size());
            callee = phoneList.get(calleeIndex);
            calleeName = phoneNameMap.get(callee);
            if (!(caller.equals(callee))) break;

        }

        String buildTime = randomBuildTime(startTime, endTime);
        DecimalFormat df = new DecimalFormat("0000");
        String duration = df.format((int) (60 * 30 * Math.random()));
        StringBuilder sb = new StringBuilder();
        sb.append(caller + ",").append(callee + ",").append(buildTime + ",").append(duration);

        return sb.toString();
    }

    /**
     * 根据传入的时间范围，随机取通话开始的时间
     */
    public String randomBuildTime(String startTime, String endTime) {

        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate = sdf1.parse(startTime);
            Date endDate = sdf1.parse(endTime);

            if (endDate.getTime() <= startDate.getTime()) return null;

            long randomTS = startDate.getTime() + (long) ((endDate.getTime() - startDate.getTime()) * Math.random());
            Date resultDate = new Date(randomTS);
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String resultTimeString = sdf2.format(resultDate);

            return resultTimeString;


        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;

    }

    /**
     * 写入日志
     * @param
     * @throws
     */
    public void writeLog(String filePath) {

        try {
            OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(filePath), "UTF-8");
            while (true) {
                Thread.sleep(500);
                String log = product();
                System.out.println(log);
                osw.write(log + '\n');
                //一定要手动flush才能确保每条数据都写入到文件一次，相当于手动保存
                osw.flush();}

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e2) {
            e2.printStackTrace();
        }

    }


    public static void main(String[] args) throws InterruptedException {

        if (args == null || args.length <= 0) {

            System.out.println("no argument");
        }

//        String logPath = "/Users/yuanzhss/IdeaProjects/ct/ct_producer/calllog.csv";
        ProductLog productLog = new ProductLog();
        productLog.initPhone();
        productLog.writeLog(args[0]);



        }

    }

