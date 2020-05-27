package kafka;

import hbase.HBaseDAO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtil;

import java.io.IOException;
import java.util.Arrays;

public class HBaseConsumer {

    public static void main(String[] args) throws IOException {

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(PropertiesUtil.properties);

        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topics")));

        HBaseDAO hb = new HBaseDAO();

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord rc : records) {
                String oriValue = rc.value().toString();
                System.out.println(rc.value());
                hb.put(oriValue);

            }

        }

    }

}
