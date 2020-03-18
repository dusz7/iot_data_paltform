package flink.operators;

import data.IoTData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

public class IoTDataUniqueStateMapFunction extends RichMapFunction<IoTData, Tuple2<IoTData, Boolean>> {

    private transient ListState<String> uniqueDataId;

    @Override
    public Tuple2<IoTData, Boolean> map(IoTData ioTData) throws Exception {
        if (!Iterables.contains(uniqueDataId.get(), ioTData.getDataId())) {
            uniqueDataId.add(ioTData.getDataId());
            return new Tuple2<>(ioTData, true);
        }
        return new Tuple2<>(ioTData, false);
    }

    @Override
    public void open(Configuration config) {
        ListStateDescriptor<String> descriptor = new ListStateDescriptor<String>("unique_data", String.class); // default value of the state
        uniqueDataId = getRuntimeContext().getListState(descriptor);
    }
}
