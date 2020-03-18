package flink.operators;

import data.IoTData;
import org.apache.flink.api.java.functions.KeySelector;

public class IoTDataKeySelector implements KeySelector<IoTData, String> {
    @Override
    public String getKey(IoTData value) throws Exception{
        return value.getDataId();
    }
}
