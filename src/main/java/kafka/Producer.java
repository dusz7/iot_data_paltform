package kafka;

import com.esotericsoftware.yamlbeans.YamlReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.IoTDataConf;
import utils.ResourcesPath;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;

import java.time.Instant;
import java.util.*;


public class Producer {

    private static Logger logger = LoggerFactory.getLogger(Producer.class);

    private static final boolean isDataFake = true;

    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) throws IOException {

        init();

        YamlReader reader = new YamlReader(new InputStreamReader(Producer.class.getResourceAsStream(ResourcesPath.CONFIG_FILE_PATH)));
        final IoTDataConf ioTDataConf = reader.read(IoTDataConf.class);

        for (int i = 1; i < 9999999; i++) {
            System.out.println("Ready to send message:");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            publishMessage(ioTDataConf.TOPIC, readData());
        }
        producer.close();

    }

    private static void init() throws IOException {
        InputStream inputStream = Producer.class.getResourceAsStream(ResourcesPath.PRODUCER_PROP_FILE_PATH);
        Properties properties = new Properties();
        properties.load(inputStream);
        producer = new KafkaProducer<String, String>(properties);
    }

    private static String readData() {
        if (isDataFake) {
            return createFakeData();
        }

        return null;
    }

    private static String createFakeData() {

        List<String> dataTypeList = Arrays.asList(new String[]{"HeartRate", "Motion", "Label", "Step"});

        try {
            String dataId = UUID.randomUUID().toString();

            Random random = new Random();
            int dataTypeIndex = random.nextInt(4);
            String dataType = dataTypeList.get(dataTypeIndex);
            JSONObject dataValue = new JSONObject();

            switch (dataTypeIndex) {
                case 0:
                    int rate = 50 + random.nextInt(75);
                    dataValue.put("rate", rate);
                    break;
                case 1:
                    DecimalFormat df = new DecimalFormat("#.000000");
                    double x = (0.001 + (0.11 - 0.001) * random.nextDouble()) * (random.nextBoolean() ? 1 : -1);
                    x = Double.valueOf(df.format(x));
                    double y = (0.001 + (0.11 - 0.001) * random.nextDouble()) * (random.nextBoolean() ? 1 : -1);
                    y = Double.valueOf(df.format(y));
                    double z = -1 + (0.1 * random.nextDouble() * (random.nextBoolean() ? 1 : -1));
                    z = Double.valueOf(df.format(z));
                    dataValue.put("x_acc", x);
                    dataValue.put("y_acc", y);
                    dataValue.put("z_acc", z);
                    break;
                case 2:
                    int stage = random.nextInt(6);
                    dataValue.put("stage", stage);
                    break;
                case 3:
                    int steps = random.nextInt(300);
                    dataValue.put("steps", steps);
                    break;
            }


            Instant instant = Instant.now();
            String dataTimeStamp = instant.toString();

            JSONObject data = new JSONObject();
            data.put("dataId", dataId);
            data.put("dataTimeStamp", dataTimeStamp);

            data.put("dataType", dataType);
            data.put("dataValue", dataValue);

//            System.out.println(data.toString());
            return data.toString();
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    private static void publishMessage(String topic, String message) {
        System.out.println(message);
        producer.send(new ProducerRecord<String, String>(topic, message));
    }
}
