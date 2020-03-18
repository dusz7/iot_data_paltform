package flink.connector;

import data.IoTData;
import flink.sink.InfluxDBSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkWithInfluxDB {
    private static Logger logger = LoggerFactory.getLogger(FlinkWithInfluxDB.class);

    public static void writeToInfluxDB(DataStream<IoTData> stream) throws Exception{
        stream.addSink(new InfluxDBSinkFunction());
    }
}
