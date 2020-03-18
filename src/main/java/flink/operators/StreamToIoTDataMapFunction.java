package flink.operators;

import data.IoTData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.JSONObject;

import java.time.Instant;

public class StreamToIoTDataMapFunction extends RichMapFunction<ObjectNode, IoTData> {

    @Override
    public IoTData map(ObjectNode value) throws Exception {
        System.out.println(value.get("dataValue").asText());
        return new IoTData(
                Instant.parse(value.get("dataTimeStamp").asText()),
                value.get("dataId").asText(),
                value.get("dataType").asText(),
                new JSONObject(value.get("dataValue").toString()));
    }
}
