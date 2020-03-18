package flink.jobs;

import data.IoTData;
import flink.connector.FlinkWithInfluxDB;
import flink.connector.FlinkWithKafka;
import flink.operators.IoTDataKeySelector;
import flink.operators.IoTDataUniqueStateMapFunction;
import flink.operators.StreamToIoTDataMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ETLJob {
    private static Logger logger = LoggerFactory.getLogger(ETLJob.class);

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<ObjectNode> sourceStream = FlinkWithKafka.readFromKafka(env);

        SingleOutputStreamOperator<IoTData> extractedIoTDataStream = extractProcess(sourceStream);
//        extractedIoTDataStream.print();

        SingleOutputStreamOperator<IoTData> filteredIoTDataStream = filterProcess(extractedIoTDataStream);
        filteredIoTDataStream.print();

        FlinkWithInfluxDB.writeToInfluxDB(filteredIoTDataStream);

        env.execute("IoT edge Streaming");
    }

    private static SingleOutputStreamOperator<IoTData> extractProcess(DataStreamSource<ObjectNode> stream) {
        SingleOutputStreamOperator<IoTData> resultStream = stream.map(new StreamToIoTDataMapFunction());
        return resultStream;
    }

    private static SingleOutputStreamOperator<IoTData> filterProcess(SingleOutputStreamOperator<IoTData> stream) {
        // deduplication by data Id
        SingleOutputStreamOperator<IoTData> reduceKeyedStream = stream
                .keyBy(new IoTDataKeySelector())
                .reduce((a,b) -> a);

        // deduplication from the same data Id
        SingleOutputStreamOperator<Tuple2<IoTData, Boolean>> dedupedKeyedStream = reduceKeyedStream
                .keyBy(new IoTDataKeySelector())
                .map(new IoTDataUniqueStateMapFunction());

        SingleOutputStreamOperator<Tuple2<IoTData, Boolean>> uniqueDataStream = dedupedKeyedStream
                .filter(p -> p.f1);

        SingleOutputStreamOperator<IoTData> filteredDataStream = uniqueDataStream
                .map(p -> p.f0);

        return filteredDataStream;
    }


}
