package flink.sink;

import data.IoTData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;

import java.util.concurrent.TimeUnit;

public class InfluxDBSinkFunction extends RichSinkFunction<IoTData> {

    private InfluxDB influxDB;

    private String dbName = "iotDataStreaming";
    private String rpName = "defaultPolicy";


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        influxDB = InfluxDBFactory.connect("http://127.0.0.1:8086");
        influxDB.createDatabase(dbName);
        influxDB.createRetentionPolicy(rpName, dbName, "30h", 1, true);
    }

    @Override
    public void close() throws Exception {
        super.close();
        influxDB.close();
    }

    @Override
    public void invoke(IoTData value, Context context) throws Exception {
        BatchPoints batchPoints = BatchPoints
                .database(dbName)
                .tag("async", "true")
                .retentionPolicy(rpName)
                .consistency(InfluxDB.ConsistencyLevel.ALL)
                .build();
        Point point = null;
        if (value.getDataType().equals("HeartRate")) {
            point = Point.measurement("heartRate")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("timeStamp", value.getDataTime().toString())
                    .addField("dataId", value.getDataId())
                    .addField("rate", value.getDataValue().getInt("rate"))
                    .build();

        } else if (value.getDataType().equals("Motion")){
            point = Point.measurement("motion")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("timeStamp", value.getDataTime().toString())
                    .addField("dataId", value.getDataId())
                    .addField("x_acc", (float) value.getDataValue().getDouble("x_acc"))
                    .addField("y_acc", (float) value.getDataValue().getDouble("y_acc"))
                    .addField("z_acc", (float) value.getDataValue().getDouble("z_acc"))
                    .build();
        } else if (value.getDataType().equals("Label")) {
            point = Point.measurement("label")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("timeStamp", value.getDataTime().toString())
                    .addField("dataId", value.getDataId())
                    .addField("stage", value.getDataValue().getInt("stage"))
                    .build();
        } else if (value.getDataType().equals("Step")) {
            point = Point.measurement("step")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("timeStamp", value.getDataTime().toString())
                    .addField("dataId", value.getDataId())
                    .addField("steps", value.getDataValue().getInt("steps"))
                    .build();
        } else {
            point = Point.measurement("other")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("timeStamp", value.getDataTime().toString())
                    .build();
        }
        batchPoints.point(point);
        influxDB.write(batchPoints);
    }
}
