package flink.connector;

import com.esotericsoftware.yamlbeans.YamlReader;
import flink.jobs.ETLJob;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.IoTDataConf;
import utils.ResourcesPath;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class FlinkWithKafka {
    private static Logger logger = LoggerFactory.getLogger(FlinkWithKafka.class);

    public static DataStreamSource<ObjectNode> readFromKafka(StreamExecutionEnvironment env) throws Exception {
        YamlReader reader = new YamlReader(
                new InputStreamReader(ETLJob.class.getResourceAsStream(ResourcesPath.CONFIG_FILE_PATH))
        );

        final IoTDataConf ioTDataConf = reader.read(IoTDataConf.class);

//        Time window = Time.seconds(ioTDataConf.WINDOW);
//        Time slide = Time.seconds(ioTDataConf.SLIDE);

        InputStream inputStream = ETLJob.class.getResourceAsStream(ResourcesPath.CONSUMER_PROP_FILE_PATH);
        Properties properties = new Properties();
        properties.load(inputStream);

        DataStreamSource<ObjectNode> sourceStream = env
                .addSource(new FlinkKafkaConsumer<ObjectNode>(ioTDataConf.TOPIC, new JsonNodeDeserializationSchema(), properties));

        return sourceStream;
    }

    public static void writeToKafka(DataStream<String> inStream) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                "iot-edge-out",
                new SimpleStringSchema(),
                properties
        );
        producer.setWriteTimestampToKafka(true);

        inStream.addSink(producer);
    }
}
